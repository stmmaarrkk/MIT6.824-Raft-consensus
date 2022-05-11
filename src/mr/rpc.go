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

type RegArgs struct {
}

type RegReply struct {
	WorkerId int
	success  bool
}

type TaskReqArgs struct {
	WorkerId int
}

type TaskReqReply struct {
	NewTask *Task
}

type TaskReportArgs struct {
	TaskId   int
	WorkerId int
	Complete bool
}

type TaskReportReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
