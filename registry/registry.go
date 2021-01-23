package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// GfaRegistry：GfaRegistry是一个简单的注册中心，提供以下功能。
// 添加服务器并接收heartbeat以使其保持活动状态。
// 同时返回所有存活的服务器和同步删除不存活的服务器。
type GfaRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_gfaRPC_/registry"
	defaultTimeout = time.Minute * 5		// 默认超时时间设置为 5 min
)

// New：新建一个具有超时设置的注册表实例
func New(timeout time.Duration) *GfaRegistry {
	return &GfaRegistry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultGeeRegister = New(defaultTimeout)

// putServer：添加服务实例，如果服务已经存在，则更新 start。
func (r *GfaRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		// 如果服务器不存在，则进行添加
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		// 如果服务器存在，则进行时间更新
		s.start = time.Now() // if exists, update start time to keep alive
	}
}

// aliveServers：返回可用的服务列表，如果存在超时的服务，则删除。
func (r *GfaRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// ServeHTTP：采用 HTTP 协议提供服务，且所有的有用信息都承载在 HTTP Header 中。
// Runs at /_gfaRPC_/registry
func (r *GfaRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// 返回所有可用的服务列表，通过自定义字段 X-Gfarpc-Servers 承载。
		// keep it simple, server is in req.Header
		w.Header().Set("X-GfaRPC-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		// 添加服务实例或发送心跳，通过自定义字段 X-Gfarpc-Server 承载。
		// keep it simple, server is in req.Header
		addr := req.Header.Get("X-GfaRPC-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleHTTP registers an HTTP handler for GeeRegistry messages on registryPath
func (r *GeeRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultGeeRegister.HandleHTTP(defaultPath)
}

// Heartbeat：每隔一段时间发送一次心跳消息
// 提供 Heartbeat 方法，便于服务启动时定时向注册中心发送心跳，默认周期比注册中心设置的过期时间少 1 min。
func Heartbeat(registry, addr string, duration time.Duration) {
	// 如果定时时间为 0，
	if duration == 0 {
		// 确保在从注册表中删除之前有足够的时间发送心跳
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

// sendHeartbeat：发送心跳
func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	// 启动一个新请求
	req, _ := http.NewRequest("POST", registry, nil)
	// 请求头设置
	req.Header.Set("X-GfaRPC-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
