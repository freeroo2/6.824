package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	id := os.Getpid()
	lastTaskID := -1
	lastTaskType := TaskType("")
	for {
		reply := ApplyForTask(id, lastTaskID, lastTaskType)
		switch reply.TaskType {
		case MAP:
			doMapWork(reply, mapf)
		case REDUCE:
			doReduceWork(reply, reducef)
		case "":
			goto End
		}
		lastTaskID = reply.TaskID
		lastTaskType = reply.TaskType
		// log.Printf("Worker %v finish map work %v\n", reply.WorkerID, reply.TaskID)
	}
End:
	// log.Printf("Worker %d 下班了", id)
}

func ApplyForTask(workerID, lastTaskID int, lastTaskType TaskType) *ApplyForTaskReply {
	// declare an argument structure.
	args := ApplyForTaskArgs{}

	// fill in the argument(s).
	args.WorkerID = workerID
	args.LastTaskID = lastTaskID
	args.LastTaskType = lastTaskType

	// declare a reply structure.
	reply := ApplyForTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.HandleApplyForTask", &args, &reply)
	if ok {
		// fmt.Println(reply.String())
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		//fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

func doMapWork(reply *ApplyForTaskReply, mapf func(string, string) []KeyValue) {
	// log.Printf("do map work %d", reply.TaskID)
	file, err := os.Open(reply.InputFile)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", reply.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.InputFile)
	}
	var outputFiles []*os.File
	for i := 0; i < reply.NReduce; i++ {
		oname := tempMapOutputFile(reply.WorkerID, reply.TaskID, i)
		ofile, _ := os.Create(oname)
		outputFiles = append(outputFiles, ofile)
	}

	kva := mapf(reply.InputFile, string(content))
	for _, kv := range kva {
		index := ihash(kv.Key) % reply.NReduce
		fmt.Fprintf(outputFiles[index], "%v\t%v\n", kv.Key, kv.Value)
		if err != nil {
			log.Fatalf("cannot write file %v correctly", reply.InputFile)
		}
	}
}

func doReduceWork(reply *ApplyForTaskReply, reducef func(string, []string) string) {
	// log.Printf("do reduce work %d", reply.TaskID)
	var intermediate []string
	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("tmp-%d-%d", i, reply.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		intermediate = append(intermediate, strings.Split(string(content), "\n")...)
	}
	var kvs []KeyValue
	for _, line := range intermediate {
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")
		kvs = append(kvs, KeyValue{
			Key:   split[0],
			Value: split[1],
		})
	}
	sort.Sort(ByKey(kvs))

	ofile, _ := os.Create(tempReduceOutputFile(reply.WorkerID, reply.TaskID))
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	ofile.Close()
}
