package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

const TimeBeforeRetryPut = 100 * time.Millisecond

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get fetches the current value and version for a key.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	var reply rpc.GetReply

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			return reply.Value, reply.Version, reply.Err
		}
	}
}

// Put updates key with value only if version matches.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	var reply rpc.PutReply
	retried := false

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrVersion {
				if retried {
					return rpc.ErrMaybe
				} else {
					return rpc.ErrVersion
				}
			}
			return reply.Err
		} else {
			retried = true
			time.Sleep(TimeBeforeRetryPut)
		}
	}
}
