package raftkv

import (
	"labrpc"
)
import "crypto/rand"
import "math/big"

var clientId int

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader  int
	id      int
	seq     uint64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.id = clientId
	clientId++
	ck.seq = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{key, ck.id, ck.seq}
	ck.seq++
	// until sent to right leader
	leader := ck.leader
	for {
		var reply GetReply
		//fmt.Printf("now client %v call %v to get %v\n",ck.id, leader, key)
		if ck.servers[leader].Call("KVServer.Get", &args, &reply) {
			//fmt.Printf("client %v get return from %v\n",ck.id,leader)
			if !reply.WrongLeader {
				ck.leader = leader
				if reply.Err=="" {
					return reply.Value
				}
			}
		}
		leader = (leader +1) % len(ck.servers)    // can't reach leader
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, ck.id, ck.seq}
	ck.seq++
	// until set to right leader
	leader := ck.leader
	for {
		var reply PutAppendReply
		//fmt.Printf("now client %v call %v to put %v %v\n",ck.id, leader, key, value)
		if ck.servers[leader].Call("KVServer.PutAppend", &args, &reply) {
			//fmt.Printf("get put reply from %v\n",ck.leader)
			if !reply.WrongLeader {
				ck.leader = leader
				if reply.Err=="" {
					return
				}
			}
		}
		leader = (leader +1) % len(ck.servers)    // can't reach leader
	}
}

func (ck *Clerk) Put(key string, value string) {
	//fmt.Printf("client put %v %v\n",key,value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
