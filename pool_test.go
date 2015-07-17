package redisq

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

func NewPool(addr, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := conn.Do("AUTH", password); err != nil {
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
