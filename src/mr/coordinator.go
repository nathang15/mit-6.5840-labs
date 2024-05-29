package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TmpDir = "tmp"
const TIMEOUT = 10

type TaskStatus int
type TaskType int
type JobStage int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

const (
	NotStarted TaskStatus = iota
	Executing
	Finished
)

type Task struct {
	Type     TaskType
	Status   TaskStatus
	Index    int
	File     string
	WorkerID int
}

type Coordinator struct {
	// Your definitions here.
	mut        sync.Mutex
	mapTask    []Task
	reduceTask []Task
	nMap       int
	nReduce    int
}

// Your code here -- RPC handlers for the worker to call.

// GetReduceCount RPC handler
func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	reply.ReduceCount = len(c.reduceTask)

	return nil
}

// RequestTask RPC handler
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mut.Lock()

	var task *Task
	if c.nMap > 0 {
		task = c.selectTask(c.mapTask, args.WorkerID)
	} else if c.nReduce > 0 {
		task = c.selectTask(c.reduceTask, args.WorkerID)
	} else {
		task = &Task{ExitTask, Finished, -1, "", -1}
	}
	reply.TaskType = task.Type
	reply.TaskID = task.Index
	reply.TaskFile = task.File

	c.mut.Unlock()
	go c.waitForTask(task)

	return nil
}

// ReportTaskDone RPC handler

func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	var task *Task
	if args.TaskType == MapTask {
		task = &c.mapTask[args.TaskID]
	} else if args.TaskType == ReduceTask {
		task = &c.reduceTask[args.TaskID]
	} else {
		fmt.Printf("Incorrect task type to report: %v\n", args.TaskType)
		return nil
	}

	if args.WorkerID == task.WorkerID && task.Status == Executing {
		task.Status = Finished
		if args.TaskType == MapTask && c.nMap > 0 {
			c.nMap--
		} else if args.TaskType == ReduceTask && c.nReduce > 0 {
			c.nReduce--
		}
	}

	reply.CanExit = c.nMap == 0 && c.nReduce == 0
	return nil
}

func (c *Coordinator) selectTask(taskList []Task, workerID int) *Task {
	var task *Task

	for i := 0; i < len(taskList); i++ {
		if taskList[i].Status == NotStarted {
			task = &taskList[i]
			task.Status = Executing
			task.WorkerID = workerID
			return task
		}
	}

	return &Task{NoTask, Finished, -1, "", -1}
}

func (c *Coordinator) waitForTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		return
	}

	<-time.After(time.Second * TIMEOUT)

	c.mut.Lock()
	defer c.mut.Unlock()

	if task.Status == Executing {
		task.Status = NotStarted
		task.WorkerID = -1
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
	// Your code here.
	c.mut.Lock()
	defer c.mut.Unlock()

	return c.nMap == 0 && c.nReduce == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	nMap := len(files)
	c.nMap = nMap
	c.nReduce = nReduce
	c.mapTask = make([]Task, 0, nMap)
	c.reduceTask = make([]Task, 0, nReduce)

	for i := 0; i < nMap; i++ {
		mTask := Task{MapTask, NotStarted, i, files[i], -1}
		c.mapTask = append(c.mapTask, mTask)
	}
	for i := 0; i < nReduce; i++ {
		rTask := Task{ReduceTask, NotStarted, i, "", -1}
		c.reduceTask = append(c.reduceTask, rTask)
	}

	c.server()

	outFiles, _ := filepath.Glob("mr-out")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TmpDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TmpDir)
	}
	err = os.Mkdir(TmpDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TmpDir)
	}

	return &c
}
