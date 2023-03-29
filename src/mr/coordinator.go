package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type WorkerStatusType int
type TaskStatusType int
type TaskType string
type StageType int

const (
	IDLE WorkerStatusType = iota
	BUSY
	DEAD
)

//const (
//	WAITING TaskStatusType = iota
//	INPROGRESS
//	COMPLETED
//)
const (
	MAP    TaskType = "MAP"
	REDUCE TaskType = "REDUCE"
	DONE   TaskType = "DONE"
)
const (
	MAPPING StageType = iota
	MAPPED
	REDUCING
	REDUCED
)

type Coordinator struct {
	// Your definitions here.
	lock        sync.Mutex
	stage       StageType
	nMap        int
	nReduce     int
	mapTasks    []*Task
	reduceTasks []*Task
	todoTasks   chan *Task
}

type Task struct {
	taskID         int
	taskType       TaskType
	inputFile      string
	workerID       int
	nReduce        int
	distributeTime time.Time
}

//type Worker struct {
//	workerID      int
//	status        WorkerStatusType
//	FinishedTasks []*Task
//	CurrentTask   *Task
//}

//func NewWorker(id int) *Worker {
//	return &Worker{
//		workerID: id,
//		status:   IDLE,
//	}
//}

func NewTask(tastID int, taskType TaskType, inputFile string, nReduce int) *Task {
	return &Task{
		taskID:    tastID,
		workerID:  -1,
		taskType:  taskType,
		inputFile: inputFile,
		nReduce:   nReduce,
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) HandleApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.LastTaskID >= 0 {
		if args.LastTaskType == MAP {
			for i := 0; i < c.nReduce; i++ {
				err := os.Rename(tempMapOutputFile(args.WorkerID, args.LastTaskID, i),
					finalMapOutputFile(args.LastTaskID, i))
				if err != nil {
					log.Fatalf("Failed to mark map output file `%s` as final: %e",
						tempMapOutputFile(args.WorkerID, args.LastTaskID, i), err)
				}
			}
		} else if args.LastTaskType == REDUCE {
			err := os.Rename(tempReduceOutputFile(args.WorkerID, args.LastTaskID),
				finalReduceOutputFile(args.LastTaskID))
			if err != nil {
				log.Fatalf("Failed to mark reduce output file `%s` as final: %e",
					tempReduceOutputFile(args.WorkerID, args.LastTaskID), err)
			}
		}
	}

	switch c.stage {
	case MAPPING:
		if len(c.mapTasks) == 0 {
			c.stage = MAPPED
		}
	case MAPPED:
		c.transitFromMapToReduce()
	case REDUCING:
		if len(c.reduceTasks) == 0 {
			c.stage = REDUCED
		}
	case REDUCED:
		close(c.todoTasks)
		return nil
	}

	task, ok := <-c.todoTasks
	if !ok {
		return nil
	}
	log.Println("拿到任务了")
	reply.TaskID = task.taskID
	reply.TaskType = task.taskType
	reply.NReduce = task.nReduce
	reply.InputFile = task.inputFile

	task.workerID = args.WorkerID
	task.distributeTime = time.Now()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	if c.stage == REDUCED {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:     MAPPING,
		nMap:      len(files),
		nReduce:   nReduce,
		todoTasks: make(chan *Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	c.lock.Lock()
	// Your code here.
	for i, file := range files {
		task := NewTask(i, MAP, file, nReduce)
		c.mapTasks = append(c.mapTasks, task)
		c.todoTasks <- task
	}
	c.lock.Unlock()
	c.server()
	// todo
	return &c
}

func (c *Coordinator) deleteTask(taskID int, taskType TaskType) {
	length := len(c.mapTasks)
	if taskType == MAP {
		if taskID >= length {
			return
		}
		if taskID == length-1 {
			c.mapTasks = append(c.mapTasks[:length-1])
		} else {
			c.mapTasks = append(c.mapTasks[:taskID], c.mapTasks[taskID+1:]...)
		}
	} else if taskType == REDUCE {
		if taskID >= length {
			return
		}
		if taskID == length-1 {
			c.reduceTasks = append(c.reduceTasks[:length-1])
		} else {
			c.reduceTasks = append(c.reduceTasks[:taskID], c.reduceTasks[taskID+1:]...)
		}
	}
}

func (c *Coordinator) transitFromMapToReduce() {
	if c.stage != MAPPED {
		return
	}
	for i := 0; i < c.nReduce; i++ {
		task := NewTask(i, REDUCE, "", c.nReduce)
		c.reduceTasks = append(c.reduceTasks, task)
		c.todoTasks <- task
	}
	c.stage = REDUCING
}

func tempMapOutputFile(workerID, mapID, reduceID int) string {
	return fmt.Sprintf("tmp-worker_%d-%d-%d", workerID, mapID, reduceID)
}

func finalMapOutputFile(mapID, reduceID int) string {
	return fmt.Sprintf("tmp-%d-%d", mapID, reduceID)
}

func tempReduceOutputFile(workerID, reduceID int) string {
	return fmt.Sprintf("tmp-worker_%d-out-%d", workerID, reduceID)
}

func finalReduceOutputFile(reduceID int) string {
	return fmt.Sprintf("mr-out-%d", reduceID)
}
