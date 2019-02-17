package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

const TIMEOUT time.Duration = 1000 // operation timeout

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
	Key       string
	Value     string
	Client    int
	Seq       uint64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        map[string]string
	notifyCH  map[int]chan bool // notify while command has been applied
	lastApply map[int]uint64
	persister *raft.Persister
}

// try append op to raft and wait for callback
func (kv *KVServer) appendRequest(op *Op) Err {
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return "wrong leader"
	}
	kv.mu.Lock()
	notify, ok := kv.notifyCH[index]
	if !ok {
		notify = make(chan bool, 1)
		kv.notifyCH[index] = notify
	}
	kv.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * TIMEOUT):
		return "timeout"
	case valid := <-notify: // applied
		if valid {
			return ""
		} else {
			return "append error"
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{"Get", args.Key, "", args.Client, args.Seq}

	reply.Err = kv.appendRequest(&op)
	reply.WrongLeader = false
	if reply.Err == "" {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.db[op.Key]
	} else if reply.Err == "wrong leader" {
		reply.WrongLeader = true
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{args.Op, args.Key, args.Value, args.Client, args.Seq}
	reply.Err = kv.appendRequest(&op)
	if reply.Err == "wrong leader" {
		reply.WrongLeader = true
	}
	reply.WrongLeader = false
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
	//kv.maxraftstate = -1
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.notifyCH = make(map[int]chan bool)
	kv.db = make(map[string]string)
	kv.lastApply = make(map[int]uint64)
	kv.persister = persister

	// You may need initialization code here.

	go kv.applyDaemon()

	return kv
}

func (kv *KVServer) applyDaemon() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if ch, ok := kv.notifyCH[msg.CommandIndex]; ok {
			if len(ch) == 0 {
				ch <- msg.CommandValid
			} else {
				delete(kv.notifyCH, msg.CommandIndex) // the waiting request already returned
			}
		}

		if msg.CommandValid {
			if msg.CommandIndex == -1 {
				kv.restoreSnapshot()
			} else if msg.Command!=nil {
				op := msg.Command.(Op)
				if seq, ok := kv.lastApply[op.Client]; !ok || seq != op.Seq {
					switch op.Operation {
					case "Get":
					case "Put":
						kv.db[op.Key] = op.Value
					case "Append":
						kv.db[op.Key] += op.Value
					}
					kv.lastApply[op.Client] = op.Seq
				}
			}
			kv.maySnapshot(msg.CommandIndex, msg.CommandTerm)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) maySnapshot(lastIndex int, lastTerm int){
	if kv.maxraftstate==-1 || kv.persister.RaftStateSize()<=kv.maxraftstate{
		return
	}
	buffer := new(bytes.Buffer)
	e:=labgob.NewEncoder(buffer)
	e.Encode(lastIndex)
	e.Encode(lastTerm)
	e.Encode(kv.db)
	e.Encode(kv.lastApply)
	kv.rf.Snapshot(lastIndex, lastTerm, buffer.Bytes())
}

func (kv *KVServer) restoreSnapshot(){
	buffer := bytes.NewBuffer(kv.persister.ReadSnapshot())
	d:=labgob.NewDecoder(buffer)
	var lastIndex, lastTerm int
	if d.Decode(&lastIndex)!=nil || d.Decode(&lastTerm)!=nil || d.Decode(&kv.db)!=nil || d.Decode(&kv.lastApply)!=nil {
		log.Panicln("restore snapshot error")
	}
}
