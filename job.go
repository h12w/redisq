package redisq

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

type Job interface {
	Do() error
}

type JobQ struct {
	Name      string
	pool      *redis.Pool
	inputQ    *Q
	consumers []*Q
	doneQ     *Q
	failQ     *Q
	newJob    func() Job
	errCh     chan error
}

type JobQError struct {
	Msg string
	Job Job
	Err error
}

func (e *JobQError) Error() string {
	return e.Msg
}

func NewJobQ(name string, consumerCount int, newJob func() Job, pool *redis.Pool) (*JobQ, error) {
	q := &JobQ{
		Name:      name,
		pool:      pool,
		inputQ:    New(name+":input", pool),
		consumers: make([]*Q, consumerCount),
		doneQ:     New(name+":done", pool),
		failQ:     New(name+":fail", pool),
		newJob:    newJob,
		errCh:     make(chan error),
	}
	for i := range q.consumers {
		q.consumers[i] = New(name+":consumer:"+strconv.Itoa(i), pool)
	}
	if err := q.resume(); err != nil {
		return nil, err
	}
	return q, nil
}

func (p *JobQ) resume() error {
	c := p.pool.Get()
	defer c.Close()
	cntKey := p.Name + ":consumer:count"
	cnt, err := redis.Int(c.Do("GET", cntKey))
	if err != nil {
		return nil // assume first time
	}
	for i := 0; i < cnt; i++ {
		n, err := p.consumers[i].Len()
		if err != nil {
			return err
		}
		for j := 0; j < n; j++ {
			if err := p.consumers[i].PopTo(p.inputQ, nil); err != nil {
				return err
			}
		}
	}
	_, err = c.Do("SET", cntKey, len(p.consumers))
	return err
}

func (p *JobQ) Put(job Job) error {
	return p.inputQ.Put(job)
}

func (p *JobQ) Consume() <-chan error {
	for _, c := range p.consumers {
		go p.process(c)
	}
	return p.errCh
}

func (p *JobQ) process(consumer *Q) {
	for {
		job := p.newJob()
		if err := p.inputQ.PopTo(consumer, &job); err != nil {
			p.sendErr("fail to get job from input queue", job, err)
			continue
		}
		if err := job.Do(); err != nil {
			p.sendErr("fail to do the job", job, err)
			if err := consumer.PopTo(p.failQ, nil); err != nil {
				p.sendErr("fail to put job into the fail queue", job, err)
			}
			continue
		}
		if err := consumer.PopTo(p.doneQ, nil); err != nil {
			p.sendErr("fail to pu job into the done queue", job, err)
		}
	}
}

func (p *JobQ) sendErr(msg string, job Job, err error) {
	p.errCh <- &JobQError{Msg: msg, Job: job, Err: err}
}
