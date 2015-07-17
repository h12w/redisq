package redisq

import (
	"encoding/json"

	"github.com/garyburd/redigo/redis"
)

type Q struct {
	Name string
	pool *redis.Pool
}

func New(name string, pool *redis.Pool) *Q {
	return &Q{
		Name: name,
		pool: pool,
	}
}

func (q *Q) Put(v interface{}) error {
	c := q.pool.Get()
	defer c.Close()
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = c.Do("LPUSH", q.Name, buf)
	return err
}

func (q *Q) Pop(v interface{}) error {
	c := q.pool.Get()
	defer c.Close()
	if v == nil {
		_, err := c.Do("LTRIM", q.Name, 0, -2)
		return err
	}
	buf, err := redis.Bytes(c.Do("RPOP", q.Name))
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

func (q *Q) Peek(v interface{}) error {
	c := q.pool.Get()
	defer c.Close()
	buf, err := redis.Bytes(c.Do("LINDEX", q.Name, -1))
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

func (q *Q) PopTo(o *Q, v interface{}) error {
	c := q.pool.Get()
	defer c.Close()
	buf, err := redis.Bytes(c.Do("BRPOPLPUSH", q.Name, o.Name, 0))
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}
	return json.Unmarshal(buf, v)
}

func (q *Q) Delete() error {
	c := q.pool.Get()
	defer c.Close()
	_, err := c.Do("DEL", q.Name)
	return err
}
