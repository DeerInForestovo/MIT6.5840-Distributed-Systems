package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu    sync.Mutex
	store map[string]ValueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		store: make(map[string]ValueVersion),
	}
	return kv
}

// Get returns the value and version for args.Key.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	vv, ok := kv.store[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = vv.Value
	reply.Version = vv.Version
	reply.Err = rpc.OK
}

// Put updates the value if version matches.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	vv, ok := kv.store[args.Key]
	if !ok {
		if args.Version == 0 {
			kv.store[args.Key] = ValueVersion{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	}

	if vv.Version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.store[args.Key] = ValueVersion{Value: args.Value, Version: vv.Version + 1}
	reply.Err = rpc.OK
}

func (kv *KVServer) Kill() {}

func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
