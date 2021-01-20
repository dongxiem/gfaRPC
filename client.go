package gfaRPC

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gfaRPC/codec"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Call represents an active RPC.
type Call struct {
	Seq           uint64
	ServiceMethod string      // 格式： "<service>.<method>"
	Args          interface{} // 函数的参数
	Reply         interface{} // 函数的回复
	Error         error       // 错误记录
	Done          chan *Call  // 使用 channel 支持异步调用。
}

// done：当调用结束时，会调用 call.done() 通知调用方
func (call *Call) done() {
	// 此处联系 Call 进行分析
	call.Done <- call
}

// Client represents an RPC Client.
// There may be multiple outstanding Calls associated
// with a single Client, and a Client may be used by
// multiple goroutines simultaneously.
type Client struct {
	cc       codec.Codec		// cc 是消息的编解码器，和服务端类似，用来序列化将要发送出去的请求，以及反序列化接收到的响应。
	opt      *Option
	sending  sync.Mutex 		// sending 是一个互斥锁，和服务端类似，为了保证请求的有序发送，即防止出现多个请求报文混淆。
	header   codec.Header		// header 是每个请求的消息头，header 只有在请求发送时才需要，而请求发送是互斥的，因此每个客户端只需要一个
	mu       sync.Mutex 		// mu 用于保护 seq 的并发
	seq      uint64
	pending  map[uint64]*Call	// pending 存储未处理完的请求，键是编号，值是 Call 实例。
	closing  bool 				// 用户主动关闭
	shutdown bool 				// 服务器告知关闭
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// Close the connection
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// IsAvailable return true if the client does work
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// registerCall：将参数 call 添加到 client.pending 中，并更新 client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	// 添加 call 的键值对映射，键为 call.Seq，值为 call
	client.pending[call.Seq] = call
	// client.seq +1
	client.seq++
	return call.Seq, nil
}

// removeCall：根据 seq，从 client.pending 中移除对应的 call，并返回。
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	// 根据 seq，取出 call
	call := client.pending[seq]
	// 移除该 call
	delete(client.pending, seq)
	// 返回该 call
	return call
}

// terminateCalls：服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call。
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		// 错误赋值
		call.Error = err
		// 调用 done 进行方法中止
		call.done()
	}
}

// send：进行发送
func (client *Client) send(call *Call) {
	// 确保客户端将发送一个完整的请求
	client.sending.Lock()
	defer client.sending.Unlock()

	// 注册 call
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 准备请求头
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 编码并发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// receive：接收响应
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// call 不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了。
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			// call 存在，但服务端处理出错，即 h.Error 不为空。
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			// call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值。
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// error occurs, so terminateCalls pending calls
	client.terminateCalls(err)
}

// Go：异步调用函数，它返回表示调用的调用结构。
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	// 返回 call 实例。
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// Call：调用命名函数，等待它完成，并返回其错误状态。
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	// Call 是对 Go 的封装，阻塞 call.Done，等待响应返回，是一个同步接口。
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		// 超时处理机制，使用 context 包实现，控制权交给用户，控制更为灵活。
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

// parseOptions：解析参数
func parseOptions(opts ...*Option) (*Option, error) {
	// if opts is nil or pass nil as parameter
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	// 得到编解码协议
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	// 发送 Option 信息给服务端
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	// 协商好消息的编解码方式之后，再创建一个子协程调用 receive() 接收响应。
	return newClientCodec(f(conn), opt), nil
}

// newClientCodec：进行 Client 编解码一些设定并创建一个子协程进行接收响应
func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	// 创建一个子协程调用 receive() 接收响应。
	go client.receive()
	return client
}

// clientResult：客户端结果结构体
type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

// dialTimeout：带有 Timeout 过期时间的 Dial
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	// 先进行参数解析
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 调用 net.DialTimeout
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// 如果客户端为 nil，关闭连接
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	// 创建 clientResult 类型的通道
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		// 接收返回结果
		ch <- clientResult{client: client, err: err}
	}()
	// 如果超时时间到了
	if opt.ConnectTimeout == 0 {
		// 将 ch 通道里面的内容赋值给 result
		result := <-ch
		// 返回该 result 当中的 client 及 err
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial：连接到指定网络地址的 RPC 服务器
func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// NewHTTPClient：通过HTTP作为传输协议创建新的客户端实例
func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))

	// 在切换到 RPC 协议之前需要成功的 HTTP 响应。
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// DialHTTP connects to an HTTP RPC server at the specified network address
// listening on the default HTTP RPC path.
func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// XDial根据第一个参数 rpcAddr 调用不同的函数来连接到 RPC 服务器。
// rpcAddr 是一种通用格式(协议@地址)表示 rpc 服务器
// eg, http@10.0.0.1:7001, tcp@10.0.0.1:9999, unix@/tmp/gfarpc.sock
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		// tcp、unix或其他传输协议
		return Dial(protocol, addr, opts...)
	}
}
