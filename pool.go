package redisq

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type Config struct {
	Addr     string
	Password string
	Name     string
}

func NewPool(config *Config) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", config.Addr)
			if err != nil {
				return nil, err
			}
			if config.Password != "" {
				if _, err := conn.Do("AUTH", config.Password); err != nil {
					conn.Close()
					return nil, err
				}
			}
			return conn, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
