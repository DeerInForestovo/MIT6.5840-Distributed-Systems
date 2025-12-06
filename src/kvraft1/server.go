package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu                sync.Mutex
	data              map[string]string
	version           map[string]rpc.Tversion
	lastAppliedSeqNum map[int64]int64
	lastPutResult     map[int64]rpc.PutReply
}

func (kv *KVServer) applyGet(args *rpc.GetArgs) rpc.GetReply {
	val, ok := kv.data[args.Key]
	if !ok {
		return rpc.GetReply{Err: rpc.ErrNoKey}
	}
	ver := kv.version[args.Key]
	return rpc.GetReply{Value: val, Version: ver, Err: rpc.OK}
}

// applyPut
func (kv *KVServer) applyPut(args *rpc.PutArgs) rpc.PutReply {
	clientId := args.ClientId
	seqNum := args.SeqNum

	if lastSeq, ok := kv.lastAppliedSeqNum[clientId]; ok {
		if seqNum <= lastSeq {
			return kv.lastPutResult[clientId]
		}
	}

	ver, ok := kv.version[args.Key]
	if !ok {
		ver = 0
	}
	reply := rpc.PutReply{}
	if args.Version != ver {
		reply.Err = rpc.ErrVersion
	} else {
		kv.data[args.Key] = args.Value
		kv.version[args.Key] = ver + 1
		reply.Err = rpc.OK
	}

	if reply.Err == rpc.OK || reply.Err == rpc.ErrVersion {
		kv.lastAppliedSeqNum[clientId] = seqNum
		kv.lastPutResult[clientId] = reply
	}

	return reply
}

// DoOp applies the operation to the key-value store.
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.applyGet(&args)
	case *rpc.GetArgs:
		return kv.applyGet(args)
	case rpc.PutArgs:
		return kv.applyPut(&args)
	case *rpc.PutArgs:
		return kv.applyPut(args)
	}
	// Should never reach here
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// encode maps
	e.Encode(kv.data)
	e.Encode(kv.version)
	e.Encode(kv.lastAppliedSeqNum)
	e.Encode(kv.lastPutResult)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var dataMap map[string]string
	var verMap map[string]rpc.Tversion
	var lastAppliedSeqNumMap map[int64]int64
	var lastPutResultMap map[int64]rpc.PutReply
	if d.Decode(&dataMap) != nil || d.Decode(&verMap) != nil || d.Decode(&lastAppliedSeqNumMap) != nil || d.Decode(&lastPutResultMap) != nil {
		return
	}
	kv.data = dataMap
	kv.version = verMap
	kv.lastAppliedSeqNum = lastAppliedSeqNumMap
	kv.lastPutResult = lastPutResultMap
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	r := rep.(rpc.GetReply)
	reply.Value = r.Value
	reply.Version = r.Version
	reply.Err = r.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	r := rep.(rpc.PutReply)
	reply.Err = r.Err
}

// Kill() is called by the tester when a KVServer instance
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})

	kv := &KVServer{me: me}

	kv.data = make(map[string]string)
	kv.version = make(map[string]rpc.Tversion)
	kv.lastAppliedSeqNum = make(map[int64]int64)
	kv.lastPutResult = make(map[int64]rpc.PutReply)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
