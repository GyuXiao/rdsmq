package redis

const (
	// DefaultIdleTimeoutSeconds 默认连接池超过 10 s 释放连接
	DefaultIdleTimeoutSeconds = 10
	// DefaultMaxActive 默认最大激活连接数是 10
	DefaultMaxActive = 100
	// DefaultMaxIdle 默认最大空闲连接数是 20
	DefaultMaxIdle = 20
)

type ClientOptions struct {
	idleTimeoutSeconds int
	maxActive          int
	maxIdle            int
	wait               bool

	network  string
	address  string
	password string
}

type ClientOption func(c *ClientOptions)

func WithIdleTimeoutSeconds(idleTimeoutSeconds int) ClientOption {
	return func(c *ClientOptions) {
		c.idleTimeoutSeconds = idleTimeoutSeconds
	}
}

func WithMaxActive(maxActive int) ClientOption {
	return func(c *ClientOptions) {
		c.maxActive = maxActive
	}
}

func WithMaxIdle(maxIdle int) ClientOption {
	return func(c *ClientOptions) {
		c.maxIdle = maxIdle
	}
}

func WithWaitMode(wait bool) ClientOption {
	return func(c *ClientOptions) {
		c.wait = wait
	}
}

func repairClient(c *ClientOptions) {
	if c.idleTimeoutSeconds < 0 {
		c.idleTimeoutSeconds = DefaultIdleTimeoutSeconds
	}
	if c.maxActive < 0 {
		c.maxActive = DefaultMaxActive
	}
	if c.maxIdle < 0 {
		c.maxActive = DefaultMaxIdle
	}
}
