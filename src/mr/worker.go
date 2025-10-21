package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const WaitingBeforeRetry = time.Second

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := RequestTask()
		switch task.TaskType {
		case MapTask:
			DoMapTask(task, mapf)
		case ReduceTask:
			DoReduceTask(task, reducef)
		case WaitTask:
			time.Sleep(WaitingBeforeRetry)
		case ExitTask:
			return
		default:
			return
		}
	}
}

func RequestTask() RequestTaskReply {
	args := ReportTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		log.Println("Coordinator unreachable, worker exiting.")
		os.Exit(0)
	}
	return reply
}

func ReportTaskDone(task RequestTaskReply) {
	args := ReportTaskArgs{
		TaskType: task.TaskType,
		TaskId:   task.TaskId,
	}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

func DoMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("Cannot open %v", task.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", task.FileName)
	}
	file.Close()

	kva := mapf(task.FileName, string(content))

	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			enc.Encode(&kv)
		}
		ofile.Close()
	}

	ReportTaskDone(task)
}

func DoReduceTask(task RequestTaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for i := 0; i < task.NMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatalf("Cannot open %v", iname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	tmpFile, err := os.CreateTemp("", fmt.Sprintf("mr-out-%d-*", task.TaskId))
	if err != nil {
		log.Fatalf("Cannot create temp file: %v", err)
	}
	defer tmpFile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	finalName := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tmpFile.Name(), finalName)

	ReportTaskDone(task)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
