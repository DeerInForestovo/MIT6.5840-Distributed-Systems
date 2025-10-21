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

// Add your RPC definitions here.

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	TaskType string // "map", "reduce", "wait", or "exit"
	FileName string
	TaskId   int
	NMap     int
	NReduce  int
}

type ReportTaskArgs struct {
	TaskType string // "map" or "reduce"
	TaskId   int
}

type ReportTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
