package redis

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"time"
)

type Client struct {
	pool *redis.Pool
	opts *ClientOptions
}

func NewClient(network, address, password string, opts ...ClientOption) *Client {
	c := &Client{
		opts: &ClientOptions{
			network:  network,
			address:  address,
			password: password,
		},
	}
	// 自定义参数
	for _, opt := range opts {
		opt(c.opts)
	}
	// 修复非法参数
	repairClient(c.opts)
	// 创建 redis 连接池
	pool := c.getRedisPool()
	// 返回 redis 客户端实例
	return &Client{
		pool: pool,
	}
}

func (c *Client) getRedisPool() *redis.Pool {
	return &redis.Pool{
		// 最大空闲连接数
		MaxIdle: c.opts.maxIdle,
		// 最大活跃连接数
		MaxActive: c.opts.maxActive,
		// 连接最长空闲时间
		IdleTimeout: time.Duration(c.opts.idleTimeoutSeconds) * time.Second,
		// 连接不够时，是阻塞等待还是返回错误
		Wait: c.opts.wait,
		// 创建连接
		Dial: func() (redis.Conn, error) {
			c, err := c.getRedisConn()
			if err != nil {
				return nil, err
			}
			return c, err
		},
		// 测试方法
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// 获取 redis 连接
func (c *Client) getRedisConn() (redis.Conn, error) {
	// address 不能为 ""，否则直接 panic
	if c.opts.address == "" {
		panic(any("Cannot get address from redis config"))
	}
	// 客户端配置
	var diaOpts []redis.DialOption
	if len(c.opts.password) > 0 {
		diaOpts = append(diaOpts, redis.DialPassword(c.opts.password))
	}

	conn, err := redis.DialContext(context.Background(), c.opts.network, c.opts.address, diaOpts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
