package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

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

type ApplyForTaskArgs struct {
	WorkerID     int
	LastTaskID   int
	LastTaskType TaskType
}

type ApplyForTaskReply struct {
	TaskID    int
	TaskType  TaskType
	NReduce   int
	InputFile string
}

func (r ApplyForTaskReply) String() string {
	return fmt.Sprintf("tastID: %d, taskType: %s, nReduce: %d, inputFile: %s", r.TaskID, r.TaskType, r.NReduce, r.InputFile)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
