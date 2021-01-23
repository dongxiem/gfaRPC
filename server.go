package gfaRPC

import (
	"encoding/json"
	"errors"
	"fmt"
	"gfaRPC/codec"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber    int           // MagicNumber marks this's a gfarpc request
	CodecType      codec.Type    // client may choose different Codec to encode body
	ConnectTimeout time.Duration // 0 means no limit
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Server：RPC Server.
type Server struct {
	serviceMap sync.Map
}

// NewServer returns a new Server.
func NewServer() *Server {
	return &Server{}
}

// DefaultServer：DefaultServer 是一个默认的 Server 实例，主要为了用户使用方便
var DefaultServer = NewServer()

// ServeConn：通过单个连接运行服务器，ServeConn 会阻塞，为连接提供服务，直到客户端挂断为止。
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	// defer 延迟调用
	defer func() { _ = conn.Close() }()
	var opt Option
	// 首先使用 json.NewDecoder 反序列化得到 Option 实例
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	// 检查 MagicNumber 和 CodeType 的值是否正确
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	// 根据 CodeType 得到对应的消息编解码器
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	// 接下来的处理交给 serverCodec
	server.serveCodec(f(conn), &opt)
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// serveCodec：进行解码
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex) // make sure to send a complete response
	wg := new(sync.WaitGroup)  // wait until all request are handled
	for {
		// readRequest：读取请求
		req, err := server.readRequest(cc)
		if err != nil {
			// 使用了 for 无限制地等待请求的到来，直到发生错误（例如连接被关闭，接收到的报文有问题等）
			if req == nil {
				break // it's not possible to recover, so close the connection
			}
			req.h.Error = err.Error()
			// sendResponse：回复请求
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		// handleRequest：处理请求，使用了协程并发执行请求
		// 处理请求是并发的，但是回复请求的报文必须是逐个发送的，并发容易导致多个回复报文交织在一起，客户端无法解析。在这里使用锁(sending)保证
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// request：存储 call 的所有信息
type request struct {
	h            *codec.Header 	// 请求头部
	argv, replyv reflect.Value	// 请求参数 和 返回值
	mtype        *methodType	// 方法类型
	svc          *service		// 服务
}

// readRequestHeader：读请求头部
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	// 使用 codec 的 读请求方法 ReadHeader，并记录在 h 中
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	// 返回请求头部 及 错误 nil
	return &h, nil
}

// findService：通过 ServiceMethod 从 serviceMap 中找到对应的 service
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	// 因为 ServiceMethod 的构成是 “Service.Method”，因此先将其分割成 2 部分，第一部分是 Service 的名称，第二部分即方法名。
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	// 现在 serviceMap 中找到对应的 service 实例
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	// 再从 service 实例的 method 中，找到对应的 methodType。
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// readRequest：读请求
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	// 先读取请求头部
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	// 进行请求结构体的请求头部赋值
	req := &request{h: h}
	// 根据 h.ServiceMethod 进行服务查找
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	// 通过 newArgv() 和 newReplyv() 两个方法创建出两个入参实例
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// make sure that argvi is a pointer, ReadBody need a pointer as parameter
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	// 然后通过 cc.ReadBody() 将请求报文反序列化为第一个入参 argv，在这里同样需要注意 argv 可能是值类型，也可能是指针类型，所以处理方式有点差异
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// handleRequest：处理请求方法
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			// 通过 req.svc.call 完成方法调用，将 replyv 传递给 sendResponse 完成序列化即可。
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

// Accept：net.Listener 作为参数，for 循环等待 socket 连接建立，并开启子协程处理，处理过程交给了 ServerConn 方法。
func (server *Server) Accept(lis net.Listener) {
	for {
		// 实现了 Accept 方式
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

// Register在服务器中发布满足以下条件的 receiver 值的方法集：
//	- 导出类型的导出方法
//	- 两个参数，都是导出类型
//	- 第二个参数是指针
//	- 一个返回值，类型为error
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

// Register publishes the receiver's methods in the DefaultServer.
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

const (
	connected        = "200 Connected to gfaRPC"
	defaultRPCPath   = "/_gfaRPC_"
	defaultDebugPath = "/debug/gfaRPC" // 为后续 DEBUG 页面预留的地址
)

// ServeHTTP：实现了一个用于响应RPC请求的http.Handler。
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

// HandleHTTP：为rpcPath上的RPC消息注册一个HTTP处理程序，和debugPath上的调试处理程序。
// 通常仍然需要在go语句中调用http.Serve（）。
func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
}

// HandleHTTP：是默认服务器注册 HTTP 处理程序的一种方便方法
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
