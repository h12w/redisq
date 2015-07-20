package redisq

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

func heartBeatExists(pool *redis.Pool, prefix string) (bool, error) {
	c := pool.Get()
	defer c.Close()
	key := prefix + ":heartbeat"
	return redis.Bool(c.Do("EXISTS", key))
}

func startHeartBeat(pool *redis.Pool, prefix string, sec int, errCh chan error) {
	c := pool.Get()
	defer c.Close()
	key := prefix + ":heartbeat"
	ticker := time.NewTicker(time.Duration(sec) * time.Second)
	go func() {
		for _ = range ticker.C {
			_, err := c.Do("SETEX", key, sec, true)
			if err != nil {
				errCh <- err
			}
		}
	}()
}
