package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser	// conn 是由构建函数传入
	buf  *bufio.Writer		// buf 是为了防止阻塞而创建的带缓冲的 Writer，提升性能
	dec  *gob.Decoder		// dec 对应 gob 的 Decoder
	enc  *gob.Encoder		// enc 对应 gob 的 Encoder
}

var _ Codec = (*GobCodec)(nil)

// NewGobCodec：根据传递进来的 conn，返回一个 GobCodec
func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

// ReadHeader：读取 Header
func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

// ReadBody：读取 body
func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

// Write：写数据
func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	if err = c.enc.Encode(h); err != nil {
		log.Println("rpc: gob error encoding header:", err)
		return
	}
	if err = c.enc.Encode(body); err != nil {
		log.Println("rpc: gob error encoding body:", err)
		return
	}
	return
}

// Close：关闭
func (c *GobCodec) Close() error {
	return c.conn.Close()
}
