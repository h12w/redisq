package redisq

import (
	"reflect"
	"testing"

	"h12.me/gspec/db/redis"
)

var redisCache = func() *redis.Redis {
	r, err := redis.New()
	if err != nil {
		panic(err)
	}
	return r
}()

func TestPutPop(t *testing.T) {
	q := newIntQ(t, "redisq-put-pop", 1, 2, 3)
	defer q.Delete()
	var i int
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expect 1 but got %d", i)
	}
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Fatalf("expect 2 but got %d", i)
	}
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 3 {
		t.Fatalf("expect 2 but got %d", i)
	}
}

type testStruct struct {
	I int
	S string
}

func TestEncoding(t *testing.T) {
	q := newQ("redisq-encoding")
	defer q.Delete()
	s := testStruct{
		I: 1,
		S: "a",
	}
	if err := q.Put(s); err != nil {
		t.Fatal(err)
	}
	var got testStruct
	if err := q.Pop(&got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, s) {
		t.Fatalf("expect %v, got %v", s, got)
	}
}

func TestPeek(t *testing.T) {
	name := "redisq-peek"
	q := newIntQ(t, name, 1, 2)
	defer q.Delete()
	for j := 0; j < 3; j++ {
		var i int
		if err := q.Peek(&i); err != nil {
			t.Fatal(err)
		}
		if i != 1 {
			t.Fatalf("expect 1 but got %d", i)
		}
	}
	var i int
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expect 1 but got %d", i)
	}
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Fatalf("expect 2 but got %d", i)
	}
}

func TestResume(t *testing.T) {
	name := "redisq-resume"
	{
		q := newIntQ(t, name, 1, 2)
		defer q.Delete()
		var i int
		if err := q.Pop(&i); err != nil {
			t.Fatal(err)
		}
		if i != 1 {
			t.Fatalf("expect 1 but got %d", i)
		}
	}
	{
		q := newQ(name)
		var i int
		if err := q.Pop(&i); err != nil {
			t.Fatal(err)
		}
		if i != 2 {
			t.Fatalf("expect 2 but got %d", i)
		}
	}
}

func TestPopDiscard(t *testing.T) {
	q := newIntQ(t, "redisq-peek", 1, 2, 3)
	defer q.Delete()
	if err := q.Pop(nil); err != nil {
		t.Fatal(err)
	}
	var i int
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Fatalf("expect 2 but got %d", i)
	}
}

func TestPopTo(t *testing.T) {
	q1 := newIntQ(t, "redisq-popto-1", 1, 2)
	defer q1.Delete()
	q2 := newIntQ(t, "redisq-popto-2", 3)
	defer q2.Delete()
	var i int
	if err := q1.PopTo(q2, &i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expect 1 but got %d", i)
	}
	if err := q1.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Fatalf("expect 2 but got %d", i)
	}
	if err := q2.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 3 {
		t.Fatalf("expect 3 but got %d", i)
	}
	if err := q2.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expect 3 but got %d", i)
	}
}

func newQ(name string) *Q {
	return New(name, redisCache.Pool())
}

func newIntQ(t *testing.T, name string, is ...int) *Q {
	q := newQ(name)
	for _, i := range is {
		if err := q.Put(i); err != nil {
			t.Fatal(err)
		}
	}
	return q
}
