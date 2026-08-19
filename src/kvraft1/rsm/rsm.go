package rsm

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Server int
	ID     uint64
	Req    any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type Result struct {
	Err   rpc.Err
	Value any
}

type WaiterInfo struct {
	ch     chan Result
	term   int
	result *Result
	op     Op
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	id            atomic.Uint64
	waiters       map[int]*WaiterInfo
	lastAppliedID int
	snapshotCond  *sync.Cond
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:            me,
		maxraftstate:  maxraftstate,
		applyCh:       make(chan raftapi.ApplyMsg),
		sm:            sm,
		waiters:       make(map[int]*WaiterInfo),
		lastAppliedID: 0,
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
		go rsm.reader()
		rsm.snapshotCond = sync.NewCond(&rsm.mu)
		if maxraftstate != -1 {
			go rsm.snapshot()
		}
	}
	return rsm
}

func (rsm *RSM) nextID() uint64 {
	return (uint64(rsm.me) << 48) | rsm.id.Add(1)
}

func (rsm *RSM) isDuplicate(commitIndex int) bool {
	if rsm.lastAppliedID >= commitIndex {
		return true
	}
	return false
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		tester.Annotate(fmt.Sprintf("Server %d", rsm.me), "trigger reader", fmt.Sprintf("index:%v, msg:%+v", rsm.lastAppliedID, msg))
		if msg.CommandValid {
			op, hasOp := msg.Command.(Op)
			result := Result{Err: rpc.OK}

			rsm.mu.Lock()
			if hasOp && !rsm.isDuplicate(msg.CommandIndex) {
				result.Value = rsm.sm.DoOp(op.Req)
				// log.Printf("===server:%v, do op msg:%+v, op:%+v, req:%T\n", rsm.me, msg, op, op.Req)
			}
			rsm.lastAppliedID = max(msg.CommandIndex, rsm.lastAppliedID)
			// log.Printf("===server:%v, applied msg:%+v, op:%+v, req:%T, applied:%v\n", rsm.me, msg, op, op.Req, rsm.lastAppliedID)
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
				tester.Annotate(fmt.Sprintf("Server %d", rsm.me), "trigger snapshot", fmt.Sprintf("index:%v, size:%v-%v", rsm.lastAppliedID, rsm.rf.PersistBytes(), rsm.maxraftstate))
				rsm.snapshotCond.Signal()
			}

			if wi, ok := rsm.waiters[msg.CommandIndex]; ok {
				if wi.op == op {
					wi.result = &result
					select {
					case wi.ch <- *wi.result:
					default:
					}
				} else {
					errResult := Result{
						Err: rpc.ErrWrongLeader,
					}
					for _, wi := range rsm.waiters {
						wi.result = &errResult
						select {
						case wi.ch <- *wi.result:
						default:
						}
					}
				}
			}
			rsm.mu.Unlock()
		} else if msg.SnapshotValid {
			rsm.mu.Lock()
			if !rsm.isDuplicate(msg.SnapshotIndex) {
				rsm.sm.Restore(msg.Snapshot)
				rsm.lastAppliedID = msg.SnapshotIndex
			}
			for index, wi := range rsm.waiters {
				if index <= msg.SnapshotIndex || wi.term < msg.SnapshotTerm {
					wi.result = &Result{Err: rpc.ErrWrongLeader}
					select {
					case wi.ch <- *wi.result:
					default:
					}
				}
			}
			// log.Printf("===server:%v, applied snapshot:%+v, applied:%v\n", rsm.me, msg, rsm.lastAppliedID)
			tester.Annotate(fmt.Sprintf("Server %d", rsm.me), "snapshot applied", fmt.Sprintf("index:%v, size:%v-%v", rsm.lastAppliedID, rsm.rf.PersistBytes(), rsm.maxraftstate))
			rsm.mu.Unlock()
		}
	}
}

func (rsm *RSM) snapshot() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for true {
		if rsm.rf.PersistBytes() < rsm.maxraftstate {
			rsm.snapshotCond.Wait()
		}
		rsm.rf.Snapshot(rsm.lastAppliedID, rsm.sm.Snapshot())

		rsm.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rsm.mu.Lock()
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	// log.Printf("%d: submit: %T(%v)", rsm.me, req, req)
	op := Op{
		Server: rsm.me,
		ID:     rsm.nextID(),
		Req:    req,
	}
	logIndex, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	ch := make(chan Result, 1)
	rsm.mu.Lock()
	rsm.waiters[logIndex] = &WaiterInfo{
		term: term,
		ch:   ch,
		op:   op,
	}
	rsm.mu.Unlock()

	for {
		select {
		case res := <-ch:
			rsm.mu.Lock()
			delete(rsm.waiters, logIndex)
			rsm.mu.Unlock()
			return res.Err, res.Value
		case <-time.After(500 * time.Millisecond): // 超时检查是否已经又结果了，如果有结果之间返回，如果没有继续等待
			// log.Printf("server:%d, timeout but no reply:%+v", rsm.me, op)
			rsm.mu.Lock()
			wi, ok := rsm.waiters[logIndex]
			if ok {
				if wi.result != nil {
					delete(rsm.waiters, logIndex)
					rsm.mu.Unlock()
					return wi.result.Err, wi.result.Value
				}
			}
			rsm.mu.Unlock()
		}
	}
}
