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
	inputQ    *Q
	consumers []*Q
	doneQ     *Q
	failQ     *Q
	newJob    func() Job
}

func NewJobQ(name string, consumerCount int, newJob func() Job, pool *redis.Pool) (*JobQ, error) {
	proxy := &JobQ{
		Name:      name,
		inputQ:    New(name+":job", pool),
		consumers: make([]*Q, consumerCount),
		doneQ:     New(name+":done", pool),
		failQ:     New(name+":done", pool),
		newJob:    newJob,
	}
	for i := range proxy.consumers {
		proxy.consumers[i] = New(name+":consumer:"+strconv.Itoa(i), pool)
	}
	// TODO: move working back to job queue
	if err := proxy.run(); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (p *JobQ) Put(job Job) error {
	return p.inputQ.Put(job)
}

func (p *JobQ) run() error {
	for _, c := range p.consumers {
		go p.process(c)
	}
	return nil
}

func (p *JobQ) process(c *Q) {
	job := p.newJob()
	for {
		if err := p.inputQ.PopTo(c, &job); err != nil {
			continue
		}
		if err := job.Do(); err != nil {
			if err := c.PopTo(p.failQ, nil); err != nil {
			}
			continue
		}
		if err := c.PopTo(p.doneQ, nil); err != nil {
		}
	}
}
