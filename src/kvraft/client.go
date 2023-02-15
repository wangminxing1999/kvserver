package kvraft

import (
	"../labrpc"
	"fmt"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu      sync.Mutex
	id      int64
	seq     int64
	// You will have to modify this struct.
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
	ck.id = nrand()
	ck.seq = 1
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
	ck.mu.Lock()
	args := GetArgs{}
	args.Client = ck.id
	args.Index = ck.seq
	args.Key = key
	ck.seq += 1
	reply := GetReply{}
	ck.mu.Unlock()
	for {
		for i := range ck.servers {
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == "Fine" {
					fmt.Printf("Get value key:%s value:%s\n", key, reply.Value)
					return reply.Value
				} else if reply.Err == "Not existed" {
					fmt.Printf("The key:%s doesn't exist\n", key)
					return ""
				}
			}
		}
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
	ck.mu.Lock()
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Client = ck.id
	args.Index = ck.seq
	ck.seq += 1
	reply := PutAppendReply{}
	ck.mu.Unlock()
	for {
		for i := range ck.servers {
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err != "Fine" {
					fmt.Println(reply.Err)
				} else {
					fmt.Printf("Success\n")
					return
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
