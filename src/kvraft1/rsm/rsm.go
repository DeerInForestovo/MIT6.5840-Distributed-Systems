package rsm

import (
	"reflect"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Me  int
	Id  int64
	Req any
}

type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	notifyCh    map[int]chan any  // Channels to notify waiting Submit calls
	pendingOps  map[int64]Op      // Track pending ops by unique Id
	lastApplied int               // Last applied log index
	persister   *tester.Persister // Persister to hold snapshot
}

// MakeRSM creates a RSM instance.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		persister:    persister,
		notifyCh:     make(map[int]chan any),
		pendingOps:   make(map[int64]Op),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.applier()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Tester Bug?
	// Sometimes req is not a pointer type
	rv := reflect.ValueOf(req)
	if rv.Kind() != reflect.Pointer {
		ptr := reflect.New(rv.Type())
		ptr.Elem().Set(rv)
		req = ptr.Interface()
	}

	op := Op{
		Me:  rsm.me,
		Id:  time.Now().UnixNano(),
		Req: req,
	}

	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	notifyCh := make(chan any, 1)
	rsm.notifyCh[index] = notifyCh
	rsm.pendingOps[op.Id] = op
	rsm.mu.Unlock()

	select {
	case result := <-notifyCh:
		curTerm, stillLeader := rsm.rf.GetState()
		if !stillLeader || curTerm != term {
			return rpc.ErrWrongLeader, nil
		}
		return rpc.OK, result
	case <-time.After(2000 * time.Millisecond):
		rsm.mu.Lock()
		delete(rsm.notifyCh, index)
		delete(rsm.pendingOps, op.Id)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
}

func (rsm *RSM) applier() {
	for msg := range rsm.applyCh {
		if msg.SnapshotValid {
			rsm.handleSnapshot(msg)
		} else if msg.CommandValid {
			rsm.handleCommand(msg)
		}
	}
}

func (rsm *RSM) handleCommand(msg raftapi.ApplyMsg) {
	rsm.mu.Lock()
	if msg.CommandIndex <= rsm.lastApplied {
		rsm.mu.Unlock()
		return
	}
	rsm.lastApplied = msg.CommandIndex

	op := msg.Command.(Op)
	ch, ok := rsm.notifyCh[msg.CommandIndex]
	rsm.mu.Unlock()

	// Apply the command to the state machine
	result := rsm.sm.DoOp(op.Req)

	rsm.mu.Lock()
	pending, exists := rsm.pendingOps[op.Id]
	if exists && pending.Me == op.Me {
		if ok {
			ch <- result
			delete(rsm.notifyCh, msg.CommandIndex)
		}
		delete(rsm.pendingOps, op.Id)
	}

	if rsm.maxraftstate != -1 && rsm.persister.RaftStateSize() > rsm.maxraftstate {
		data := rsm.sm.Snapshot()
		rsm.rf.Snapshot(msg.CommandIndex, data)
	}
	rsm.mu.Unlock()
}

func (rsm *RSM) handleSnapshot(msg raftapi.ApplyMsg) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	print("restore??\n")

	if msg.SnapshotIndex <= rsm.lastApplied {
		return
	}

	// Restore the state machine from snapshot
	rsm.sm.Restore(msg.Snapshot)

	rsm.lastApplied = msg.SnapshotIndex
	for index := range rsm.notifyCh {
		if index <= rsm.lastApplied {
			delete(rsm.notifyCh, index)
		}
	}
}
