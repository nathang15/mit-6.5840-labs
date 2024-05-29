package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type GetReduceCountArgs struct {
}

type GetReduceCountReply struct {
	ReduceCount int
}

type RequestTaskArgs struct {
	WorkerID int
}

type RequestTaskReply struct {
	TaskType TaskType
	TaskID   int
	TaskFile string
}

type ReportTaskArgs struct {
	WorkerID int
	TaskType TaskType
	TaskID   int
}

type ReportTaskReply struct {
	CanExit bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
