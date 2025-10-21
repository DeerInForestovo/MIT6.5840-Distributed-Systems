package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskIdle = iota
	TaskInProgress
	TaskCompleted
)

const (
	MappingPhase = iota
	ReducingPhase
	Done
)

const (
	MapTask    = "map"
	ReduceTask = "reduce"
	WaitTask   = "wait"
	ExitTask   = "exit"
)

const TaskTimeout = 10 * time.Second

type Task struct {
	TaskType string
	FileName string
	TaskId   int
	State    int
	StartAt  time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
	phase       int
}

// Your code here -- RPC handlers for the worker to call.

// assign a task to a worker
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == MappingPhase {
		for i := range c.mapTasks {
			task := &c.mapTasks[i]
			if task.State == TaskIdle {
				task.State = TaskInProgress
				task.StartAt = time.Now()
				reply.TaskType = MapTask
				reply.FileName = task.FileName
				reply.TaskId = task.TaskId
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				go c.checkTimeout(task)
				return nil
			}
		}
		if !c.allTasksDone(c.mapTasks) {
			reply.TaskType = WaitTask
			return nil
		}
		c.phase = ReducingPhase
	}

	if c.phase == ReducingPhase {
		for i := range c.reduceTasks {
			task := &c.reduceTasks[i]
			if task.State == TaskIdle {
				task.State = TaskInProgress
				task.StartAt = time.Now()
				reply.TaskType = ReduceTask
				reply.TaskId = task.TaskId
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				go c.checkTimeout(task)
				return nil
			}
		}
		if !c.allTasksDone(c.reduceTasks) {
			reply.TaskType = WaitTask
			return nil
		}
		c.phase = Done
	}

	reply.TaskType = ExitTask
	return nil
}

// worker reports task completion
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask && c.phase == MappingPhase {
		c.mapTasks[args.TaskId].State = TaskCompleted
	} else if args.TaskType == ReduceTask && c.phase == ReducingPhase {
		c.reduceTasks[args.TaskId].State = TaskCompleted
	}
	return nil
}

// check if all tasks in a phase are done
func (c *Coordinator) allTasksDone(tasks []Task) bool {
	for _, t := range tasks {
		if t.State != TaskCompleted {
			return false
		}
	}
	return true
}

// reassign a task if the worker times out
func (c *Coordinator) checkTimeout(task *Task) {
	time.Sleep(TaskTimeout)
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.State == TaskInProgress && time.Since(task.StartAt) > TaskTimeout {
		task.State = TaskIdle
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == Done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:        len(files),
		nReduce:     nReduce,
		phase:       MappingPhase,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	for i, f := range files {
		c.mapTasks[i] = Task{TaskType: MapTask, FileName: f, TaskId: i, State: TaskIdle}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{TaskType: ReduceTask, TaskId: i, State: TaskIdle}
	}

	c.server()
	return &c
}
