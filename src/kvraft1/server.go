package kvraft

import (
	"log"

	"6.5840/kvraft1/rsm"
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	kvStore *kvsrv.KVServer
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	// log.Printf("server:%d doop:%T", kv.me, req)
	switch req := req.(type) {
	case rpc.GetArgs:
		args := req
		reply := rpc.GetReply{}
		kv.kvStore.Get(&args, &reply)
		return reply
	case rpc.PutArgs:
		args := req
		reply := rpc.PutReply{}
		kv.kvStore.Put(&args, &reply)
		// log.Printf("======server:%d, args:%+v put reply:%+v", kv.me, args, reply)
		return reply
	default:
		log.Fatalf("req type invalid:%T", req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return kv.kvStore.Snapshot()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.kvStore.Restore(data)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	resultReply := result.(rpc.GetReply)
	reply.Err = resultReply.Err
	reply.Value = resultReply.Value
	reply.Version = resultReply.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		// log.Printf("server:%d, put finish err:%+v", kv.me, reply)
		return
	}
	resultReply := result.(rpc.PutReply)
	reply.Err = resultReply.Err
	// log.Printf("server:%d, put finish:%+v", kv.me, reply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.kvStore = kvsrv.MakeKVServer()
	kv.Restore(persister.ReadSnapshot())
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
