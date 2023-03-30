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
	REDUCING
	STOP
)

type Coordinator struct {
	// Your definitions here.
	lock        sync.RWMutex
	stage       StageType
	nMap        int
	nReduce     int
	mapTasks    map[int]*Task
	reduceTasks map[int]*Task
	todoTasks   chan *Task
}

type Task struct {
	taskID    int
	taskType  TaskType
	inputFile string
	workerID  int
	nReduce   int
	deadLine  time.Time
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

func NewTask(taskID int, taskType TaskType, inputFile string, nReduce int) *Task {
	return &Task{
		taskID:    taskID,
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
	// log.Printf("receive ask, lastTaskID: %d, type: %s", args.LastTaskID, args.LastTaskType)
	if args.LastTaskID >= 0 {
		c.lock.Lock()
		if args.LastTaskType == MAP {
			for i := 0; i < c.nReduce; i++ {
				err := os.Rename(tempMapOutputFile(args.WorkerID, args.LastTaskID, i),
					finalMapOutputFile(args.LastTaskID, i))
				if err != nil {
					log.Fatalf("Failed to mark map output file `%s` as final: %e",
						tempMapOutputFile(args.WorkerID, args.LastTaskID, i), err)
				}
			}
			delete(c.mapTasks, args.LastTaskID)
		} else if args.LastTaskType == REDUCE {
			err := os.Rename(tempReduceOutputFile(args.WorkerID, args.LastTaskID),
				finalReduceOutputFile(args.LastTaskID))
			if err != nil {
				log.Fatalf("Failed to mark reduce output file `%s` as final: %e",
					tempReduceOutputFile(args.WorkerID, args.LastTaskID), err)
			}
			delete(c.reduceTasks, args.LastTaskID)
		}

		// log.Printf("delete task %d, remain tasks: %d", args.LastTaskID, len(c.mapTasks))
		if len(c.mapTasks) == 0 && c.stage == MAPPING {
			c.transitFromMapToReduce()
		} else if len(c.reduceTasks) == 0 && c.stage == REDUCING {
			c.transitFromReduceToStop()
		}
		c.lock.Unlock()
	}

	task, ok := <-c.todoTasks
	if !ok {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	reply.TaskID = task.taskID
	reply.TaskType = task.taskType
	reply.NMap = c.nMap
	reply.NReduce = task.nReduce
	reply.InputFile = task.inputFile
	reply.WorkerID = args.WorkerID

	task.workerID = args.WorkerID
	task.deadLine = time.Now().Add(10 * time.Second)

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

//func (c *Coordinator) stop() {
//	select {
//	case <-c.closed:
//		return
//	default:
//		close(c.closed)
//	}
//	c.waitForQuit.Wait()
//	close(c.todoTasks)
//	return
//}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.stage == STOP {
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
		stage:       MAPPING,
		nMap:        len(files),
		nReduce:     nReduce,
		todoTasks:   make(chan *Task, int(math.Max(float64(len(files)), float64(nReduce)))),
		mapTasks:    make(map[int]*Task),
		reduceTasks: make(map[int]*Task),
		// closed:      make(chan struct{}),
	}
	c.lock.Lock()
	// Your code here.
	for i, file := range files {
		task := NewTask(i, MAP, file, nReduce)
		c.mapTasks[i] = task
		c.todoTasks <- task
	}
	c.lock.Unlock()
	c.server()
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			if c.stage == MAPPING {
				for _, task := range c.mapTasks {
					if task.workerID != -1 && !task.deadLine.IsZero() {
						if time.Now().After(task.deadLine) {
							task.workerID = -1
							c.todoTasks <- task
						}
					}
				}
			} else if c.stage == REDUCING {
				for _, task := range c.reduceTasks {
					if task.workerID != -1 && !task.deadLine.IsZero() {
						if time.Now().After(task.deadLine) {
							task.workerID = -1
							c.todoTasks <- task
						}
					}
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}

func (c *Coordinator) transitFromMapToReduce() {
	// log.Println("enter transitFromMapToReduce")
	for i := 0; i < c.nReduce; i++ {
		task := NewTask(i, REDUCE, "", c.nReduce)
		c.reduceTasks[i] = task
		c.todoTasks <- task
	}
	c.stage = REDUCING
}

func (c *Coordinator) transitFromReduceToStop() {
	log.Println("enter transitFromReduceToStop")
	c.stage = STOP
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
