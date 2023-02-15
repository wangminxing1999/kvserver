package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"sync"
)
import "strconv"

var mutex sync.Mutex

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
//Request true: request for a task, false: respond to master which task is finished
const (
	maptask = iota
	reducetask
	wait
	end
)

type ReportArgs struct {
	TaskType int
	TaskID   int
}

type ReportReply struct {
}

type Args struct {
	TaskType int
	TaskID   int
}

//assume 1 is map, 2 is reduce

type Reply struct {
	TaskType   int
	TaskID     int
	Title      string
	MapTaskNum int
	NReduce    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
