package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

const TIMEOUT = 1000  // operation timeout

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db       map[string]string
	notifyCH map[int]chan bool // notify command is applied
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{"Get", args.Key, ""}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	timeout := time.NewTimer(time.Millisecond*time.Duration(TIMEOUT))
	//fmt.Printf("get lock\n")
	kv.mu.Lock()
	notify ,ok := kv.notifyCH[index]
	if !ok {
		notify = make(chan bool)
		kv.notifyCH[index] = notify
	}
	kv.mu.Unlock()
	select {
	case <- timeout.C:
		reply.Err = "timeout"
	case valid := <- notify:
		//fmt.Printf("notified get")
		if valid {
			kv.mu.Lock()
			reply.Value = kv.db[args.Key]
			kv.mu.Unlock()
			//fmt.Printf("return %v %v\n",args.Key, reply.Value)
		} else {
			reply.Err = "append error"
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Op, args.Key, args.Value}
	//fmt.Printf("try %v %v\n",op.Key,op.Value)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		//fmt.Printf("%v is not leader\n",kv.me)
		return
	}
	reply.WrongLeader = false

	timeout := time.NewTimer(time.Millisecond*time.Duration(TIMEOUT))

	kv.mu.Lock()
	notify ,ok := kv.notifyCH[index]
	if !ok {
		notify = make(chan bool)
		kv.notifyCH[index] = notify
	}
	kv.mu.Unlock()
	select {
	case <- timeout.C:
		reply.Err = "timeout"
		fmt.Printf("%v timeout\n",kv.me)
	case valid := <- notify:
		//fmt.Printf("%v notify put\n",kv.me)
		if !valid {
			fmt.Printf("%v append error\n",kv.me)
			reply.Err = "append error"
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.notifyCH = make(map[int]chan bool)
	kv.db = make(map[string]string)

	// You may need initialization code here.

	go kv.applyDaemon()

	return kv
}

func (kv *KVServer) applyDaemon() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if ch, ok := kv.notifyCH[msg.CommandIndex]; ok {
			ch <- msg.CommandValid
		}

		if msg.CommandValid {
			op := msg.Command.(Op)
			switch op.Operation {
			case "Get":
				kv.mu.Unlock()
				continue      // get op done by leader
			case "Put":
				//fmt.Printf("%v put %v %v\n", kv.me, op.Key, op.Value)
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			}
		}
		kv.mu.Unlock()
	}
}
