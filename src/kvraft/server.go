package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu               sync.Mutex
	me               int
	rf               *raft.Raft
	applyCh          chan raft.ApplyMsg
	dead             int32 // set by Kill()
	database         map[string]string
	message_chan_map map[int]chan raft.ApplyMsg
	ClientMaxApplied map[int64]int64
	maxraftstate     int // snapshot if log grows this big
	rf_term          int
	rf_leader        bool
	// Your definitions here.
}

func (kv *KVServer) getSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.ClientMaxApplied)

	return w.Bytes()
}

func (kv *KVServer) LogLengthDetector() {
	for {
		time.Sleep(time.Millisecond * 5)
		kv.mu.Lock()
		if kv.maxraftstate == -1 {
			kv.mu.Unlock()
			continue
		}
		if kv.rf.GetPersister().RaftStateSize() >= kv.maxraftstate {
			snapshot := kv.getSnapshot()
			if snapshot != nil {
				kv.rf.DeleteLog(kv.rf.GetLastApplied(), snapshot)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	fmt.Printf("receive Get request\n")
	Command := raft.Op{}
	Command.Operation = "Get"
	Command.Key = args.Key
	index, OldTerm, isLeader := kv.rf.Start(Command)
	if !isLeader {
		reply.Err = "This server is not leader!"
		return
	}
	kv.mu.Lock()
	ch := make(chan raft.ApplyMsg)
	kv.message_chan_map[index] = ch
	kv.mu.Unlock()
	timeout := time.After(50000 * time.Millisecond)
	select {
	case <-ch:
		kv.mu.Lock()
		val, ok := kv.database[args.Key]
		if ok {
			reply.Err = "Fine"
			reply.Value = val
		} else {
			reply.Err = "Not existed"
		}
		kv.mu.Unlock()
	case <-timeout:
		reply.Err = "Timeout"
	}
	CurrentTerm, isLeader2 := kv.rf.GetState()
	if !isLeader2 || OldTerm != CurrentTerm {
		reply.Err = "Leader Change"
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	fmt.Printf("receive PutAppend Request\n")
	Command := raft.Op{}
	Command.Client = args.Client
	Command.Seq = args.Index
	if args.Op == "Put" {
		Command.Operation = "Put"
		Command.Key = args.Key
		Command.Value = args.Value
	} else {
		Command.Operation = "Append"
		Command.Key = args.Key
		Command.Value = args.Value
	}
	index, OldTerm, isLeader := kv.rf.Start(Command)
	if !isLeader {
		reply.Err = "This server is not leader!"
		return
	}
	kv.mu.Lock()
	ch := make(chan raft.ApplyMsg)
	kv.message_chan_map[index] = ch
	kv.mu.Unlock()
	timeout := time.After(50000 * time.Millisecond)
	select {
	case <-ch:
		reply.Err = "Fine"
	case <-timeout:
		reply.Err = "Timeout"
	}
	CurrentTerm, isLeader2 := kv.rf.GetState()
	if !isLeader2 || OldTerm != CurrentTerm {
		reply.Err = "Leader Change"
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyLog() {
	for {
		received_appliedLog := <-kv.applyCh
		kv.mu.Lock()
		if received_appliedLog.IsSnapshot == true {
			r := bytes.NewBuffer(received_appliedLog.Snapshot)
			d := labgob.NewDecoder(r)
			var database map[string]string
			var clientmaxapplied map[int64]int64
			if d.Decode(&database) == nil && d.Decode(&clientmaxapplied) == nil {
				kv.database, kv.ClientMaxApplied = database, clientmaxapplied
			}
			kv.mu.Unlock()
			continue
		}
		channel, ok := kv.message_chan_map[received_appliedLog.CommandIndex]
		if ok {
			channel <- raft.ApplyMsg{}
		}
		if received_appliedLog.Command == nil {
			continue
		}
		appliedCommand := received_appliedLog.Command.(raft.Op)
		if kv.ClientMaxApplied[appliedCommand.Client] >= appliedCommand.Seq {
			kv.mu.Unlock()
			continue
		}
		if appliedCommand.Operation == "Put" {
			kv.database[appliedCommand.Key] = appliedCommand.Value
		} else if appliedCommand.Operation == "Append" {
			kv.database[appliedCommand.Key] += appliedCommand.Value
		}
		kv.ClientMaxApplied[appliedCommand.Client] = appliedCommand.Seq
		kv.mu.Unlock()
	}
}

func (kv *KVServer) listenStateChange() {
	for {
		time.Sleep(time.Millisecond * 10)
		kv.mu.Lock()
		CurrTerm, isLeader := kv.rf.GetState()
		if (isLeader == true && kv.rf_leader == false) || (isLeader == true && kv.rf_leader == true && CurrTerm > kv.rf_term) {
			Command := raft.Op{}
			kv.rf.Start(Command)
		}
		kv.rf_term = CurrTerm
		kv.rf_leader = isLeader
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(raft.Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.message_chan_map = make(map[int]chan raft.ApplyMsg)
	kv.ClientMaxApplied = make(map[int64]int64)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf_term, kv.rf_leader = kv.rf.GetState()
	snapshot := kv.rf.GetPersister().ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var database map[string]string
		var clientmaxapplied map[int64]int64
		if d.Decode(&database) == nil && d.Decode(&clientmaxapplied) == nil {
			kv.database, kv.ClientMaxApplied = database, clientmaxapplied
		}
	}
	// You may need initialization code here.
	go kv.applyLog()
	go kv.listenStateChange()
	go kv.LogLengthDetector()
	return kv
}
