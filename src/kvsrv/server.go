package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu        sync.Mutex
	store     map[string]string
	tokenMap  map[string]struct{}
	appendMap map[string]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.store[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.check(args.Token) {
		kv.store[args.Key] = args.Value
	}
}

// Since the append needs to return the value before doing sth else, we have to keep track

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	prev := kv.store[args.Key]
	if !kv.check(args.Token) {
		kv.appendMap[args.Token] = prev
		kv.store[args.Key] = prev + args.Value
	} else {
		prev = kv.appendMap[args.Token]
	}

	reply.Value = prev
}

func (kv *KVServer) check(token string) bool {
	// The server records all tokens, and for put and append, verify
	// if the token exists, and if it is, continue
	_, ok := kv.tokenMap[token]
	kv.tokenMap[token] = struct{}{}
	return ok
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.store = map[string]string{}
	kv.tokenMap = map[string]struct{}{}
	kv.appendMap = map[string]string{}

	return kv
}
