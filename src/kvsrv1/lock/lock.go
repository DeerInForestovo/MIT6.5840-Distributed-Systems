package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	ck kvtest.IKVClerk
	l  string
}

const TimeBeforeRetryAcquireLock = 10 * time.Millisecond

func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, l: l}
	return lk
}

// Acquire tries to grab the lock by atomically creating the key.
func (lk *Lock) Acquire() {
	for {
		_, _, err := lk.ck.Get(lk.l)

		switch err {
		case rpc.ErrNoKey:
			putErr := lk.ck.Put(lk.l, "1", 0)
			if putErr == rpc.OK {
				// success
				return
			}
			// otherwise, someone raced with us, retry
		case rpc.OK:
			// Lock exists, wait and retry
			time.Sleep(TimeBeforeRetryAcquireLock)
		default:
			// transient error, retry
			time.Sleep(TimeBeforeRetryAcquireLock)
		}
	}
}

// Release clears the lock value, making it available again.
func (lk *Lock) Release() {
	for {
		_, version, err := lk.ck.Get(lk.l)
		switch err {
		case rpc.ErrNoKey:
			return
		case rpc.OK:
			putErr := lk.ck.Put(lk.l, "", version)
			if putErr == rpc.OK {
				return
			}
		}
		time.Sleep(TimeBeforeRetryAcquireLock)
	}
}
