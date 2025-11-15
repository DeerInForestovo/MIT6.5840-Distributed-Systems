package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string

	lastLeader int // last known leader server index
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, lastLeader: 0}
	return ck
}

// Get fetches the current value and version for a key.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	for {
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[ck.lastLeader], "KVServer.Get", args, &reply)
		if ok && reply.Err == rpc.OK {
			return reply.Value, reply.Version, rpc.OK
		}
		if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{Key: key, Value: value, Version: version}
	retried := false
	for {
		server := ck.lastLeader
		for range ck.servers {
			var reply rpc.PutReply
			ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					server = (server + 1) % len(ck.servers)
					continue
				}
				ck.lastLeader = server
				if reply.Err == rpc.ErrVersion {
					if retried {
						return rpc.ErrMaybe
					}
					return rpc.ErrVersion
				}
				return reply.Err
			}
			server = (server + 1) % len(ck.servers)
		}
		retried = true
		time.Sleep(100 * time.Millisecond)
	}
}
