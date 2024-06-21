package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	DoOnce
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	DoOnce
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}

// Each request carries a token that identifies the request
type DoOnce struct {
	Token string
}
