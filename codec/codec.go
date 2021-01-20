package codec

import (
	"io"
)

type Header struct {
	ServiceMethod string // format "Service.Method"，ServiceMethod 是服务名和方法名
	Seq           uint64 // sequence number chosen by client，Seq 是请求的序号，也可以认为是某个请求的 ID，用来区分不同的请求。
	Error         string // Error 是错误信息，客户端置为空，服务端如果如果发生错误，将错误信息置于 Error 中。
}

// Codec：抽象出对消息体进行编解码的接口 Codec，为了实现不同的 Codec 实例
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

// NewCodecFunc：抽象出 Codec 的构造函数
type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	// 定义了 2 种 Codec，Gob 和 Json，但是实际代码中只实现了 Gob 一种
	GobType  Type = "application/gob"
	JsonType Type = "application/json" // not implemented
)

var NewCodecFuncMap map[Type]NewCodecFunc

// init：构建一个map，并添加 GobType 的映射
func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
