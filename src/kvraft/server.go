package raftkv

import (
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
	Client int
	Seq    uint64
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
	lastApply map[int]uint64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//kv.mu.Lock()
	//if seq,ok := kv.lastApply[args.Client];ok&&seq==args.Seq {
	//	reply.WrongLeader = false
	//	reply.Err = ""
	//	reply.Value = kv.db[args.Key]
	//	kv.mu.Unlock()
	//	return
	//}
	//kv.mu.Unlock()

	op := Op{"Get", args.Key, "", args.Client, args.Seq}
	//fmt.Printf("%v start Get\n",kv.me)
	index, _, isLeader := kv.rf.Start(op)
	//fmt.Printf("%v Get after append to leader\n",kv.me)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	timeout := time.NewTimer(time.Millisecond*time.Duration(TIMEOUT))
	//fmt.Printf("%v get to aquire lock\n",kv.me)
	kv.mu.Lock()
	//fmt.Printf("%v get got lock\n",kv.me)
	notify ,ok := kv.notifyCH[index]
	if !ok {
		notify = make(chan bool, 1)
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
	//fmt.Printf("%v return get\n",kv.me)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// this request is already applied before
	//kv.mu.Lock()
	//if seq,ok := kv.lastApply[args.Client];ok&&seq==args.Seq {
	//	reply.WrongLeader = false
	//	reply.Err = ""
	//	kv.mu.Unlock()
	//	return
	//}
	//kv.mu.Unlock()

	op := Op{args.Op, args.Key, args.Value, args.Client, args.Seq}
	//fmt.Printf("server %v try %v %v %v\n",kv.me, op.Operation, op.Key,op.Value)
	index, _, isLeader := kv.rf.Start(op)
	//fmt.Printf("server %v put after start\n",kv.me)
	if !isLeader {
		reply.WrongLeader = true
		//fmt.Printf("%v is not leader\n",kv.me)
		return
	}
	reply.WrongLeader = false

	timeout := time.NewTimer(time.Millisecond*time.Duration(TIMEOUT))

	//fmt.Printf("%v try lock\n",kv.me)
	kv.mu.Lock()
	//fmt.Printf("%v lock success\n",kv.me)

	notify ,ok := kv.notifyCH[index]
	if !ok {
		notify = make(chan bool, 1)
		kv.notifyCH[index] = notify
	}
	kv.mu.Unlock()
	//fmt.Printf("%v wait notify\n",kv.me)
	select {
	case <- timeout.C:
		reply.Err = "timeout"
		//fmt.Printf("%v timeout\n",kv.me)
	case valid := <- notify:
		//fmt.Printf("%v notify put\n",kv.me)
		if !valid {
			//fmt.Printf("%v append error\n",kv.me)
			reply.Err = "append error"
		}
	}
	//fmt.Printf("%v put return\n",kv.me)
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
	kv.lastApply = make(map[int]uint64)

	// You may need initialization code here.

	go kv.applyDaemon()

	return kv
}

func (kv *KVServer) applyDaemon() {
	for {
		//fmt.Printf("%v get msg from channel\n",kv.me)
		msg := <-kv.applyCh
		//fmt.Printf("%v got msg from channel\n",kv.me)
		kv.mu.Lock()
		//fmt.Printf("%v got lock after got msg\n",kv.me)
		if ch, ok := kv.notifyCH[msg.CommandIndex]; ok {
			if len(ch)==0 {
				ch <- msg.CommandValid
			} else {
				delete(kv.notifyCH, msg.CommandIndex)    // the waiting request already returned
			}
		}
		//fmt.Printf("%v before command valid\n", kv.me)

		if msg.CommandValid {
			op := msg.Command.(Op)
			if seq, ok:=kv.lastApply[op.Client];!ok||seq!=op.Seq {
				switch op.Operation {
				case "Get":
				case "Put":
					//fmt.Printf("%v put %v %v\n", kv.me, op.Key, op.Value)
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				}
				kv.lastApply[op.Client] = op.Seq
			}
		}
		kv.mu.Unlock()
		//fmt.Printf("%v unlock after apply\n",kv.me)
	}
}
