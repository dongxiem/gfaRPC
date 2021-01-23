package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect     SelectMode = iota // 随机选择
	RoundRobinSelect                   // 用 Robbin 算法选择
)

type Discovery interface {
	Refresh() error 						// 从远程注册表刷新
	Update(servers []string) error			// 手动更新服务列表
	Get(mode SelectMode) (string, error)	// 根据负载均衡策略，选择一个服务实例
	GetAll() ([]string, error)				// 返回所有的服务实例
}

var _ Discovery = (*MultiServersDiscovery)(nil)

// MultiServersDiscovery：是一种针对多个服务器的发现，它不需要注册中心用户显式地提供服务器地址。
type MultiServersDiscovery struct {
	r       *rand.Rand   	// 是一个产生随机数的实例，初始化时使用时间戳设定随机数种子，避免每次产生相同的随机数序列
	mu      sync.RWMutex 	// 并发保护
	servers []string
	index   int 			// 记录 Round Robin 算法已经轮询到的位置，为了避免每次从 0 开始，初始化时随机设定一个值。
}

// Refresh：刷新对于 MultiServersDiscovery 没有意义，忽略
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// Update：如果需要，动态更新发现服务器
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

// Get：根据模式获取服务器
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	case RandomSelect:
		// 如果是随机选择
		return d.servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		// 如果是 Robbin 算法选择
		s := d.servers[d.index%n] // 服务器可以更新，所以模式 n 可以确保安全
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

// GetAll：返回发现中的所有服务器
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	// return a copy of d.servers
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}

// NewMultiServerDiscovery：创建 MultiServersDiscovery 实例
func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}
