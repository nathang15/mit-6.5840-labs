package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var nReduce int

const TASK_INTERVAL = 200

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

	n, ok := getReduceCount()
	if !ok {
		fmt.Println("Failed to get reduce task count, exiting!")
		return
	}

	nReduce = n

	for {
		reply, ok := requestTask()

		if !ok {
			fmt.Println("Failed to contact coordinator.")
		}
		if reply.TaskType == ExitTask {
			fmt.Println("Alls tasks done!")
			return
		}

		exit, ok := false, true
		if reply.TaskType == NoTask {
			// entire batch is not done but there are still map or reduce tasks running
		} else if reply.TaskType == MapTask {
			doMap(mapf, reply.TaskFile, reply.TaskID)
			exit, ok = reportTaskDone(MapTask, reply.TaskID)
		} else if reply.TaskType == ReduceTask {
			doReduce(reducef, reply.TaskID)
			exit, ok = reportTaskDone(ReduceTask, reply.TaskID)
		}

		if exit || !ok {
			fmt.Println("Coordinator stops or all works are done.")
			return
		}

		time.Sleep(time.Millisecond * TASK_INTERVAL)

	}

}

func doMap(mapf func(string, string) []KeyValue, filePath string, mapID int) {
	file, err := os.Open(filePath)
	checkError(err, "Cannot open file %v\n", filePath)

	content, err := io.ReadAll(file)
	checkError(err, "Cannot read file %v\n", filePath)
	file.Close()

	kvArray := mapf(filePath, string(content))
	writeMapOutput(kvArray, mapID)
}

func writeMapOutput(kvArray []KeyValue, mapID int) {
	prefix := fmt.Sprintf("%v/mr-%v", TmpDir, mapID)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		checkError(err, "Cannot create file %v\n", filePath)
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	for _, kv := range kvArray {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		checkError(err, "Cannot encode %v to file\n", kv)
	}

	for i, buf := range buffers {
		err := buf.Flush()
		checkError(err, "Cannot flush buffer for file: %v\n", files[i].Name())
	}

	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		checkError(err, "Cannot rename file %v\n", file.Name())
	}
}

func doReduce(reducef func(string, []string) string, reduceId int) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TmpDir, "*", reduceId))
	if err != nil {
		checkError(err, "Cannot list reduce files")
	}

	kvMap := make(map[string][]string)
	var kv KeyValue

	for _, filePath := range files {
		file, err := os.Open(filePath)
		checkError(err, "Cannot open file %v\n", filePath)

		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			checkError(err, "Cannot decode from file %v\n", filePath)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	writeReduceOutput(reducef, kvMap, reduceId)
}

func writeReduceOutput(reducef func(string, []string) string,
	kvMap map[string][]string, reduceId int) {

	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TmpDir, reduceId, os.Getpid())
	file, err := os.Create(filePath)
	checkError(err, "Cannot create file %v\n", filePath)

	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
		checkError(err, "Cannot write mr output (%v, %v) to file", k, v)
	}

	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
	checkError(err, "Cannot rename file %v\n", filePath)
}

func getReduceCount() (int, bool) {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	succ := call("Coordinator.GetReduceCount", &args, &reply)

	return reply.ReduceCount, succ
}

func requestTask() (*RequestTaskReply, bool) {
	args := RequestTaskArgs{os.Getpid()}
	reply := RequestTaskReply{}
	succ := call("Coordinator.RequestTask", &args, &reply)

	return &reply, succ
}

func reportTaskDone(taskType TaskType, taskId int) (bool, bool) {
	args := ReportTaskArgs{os.Getpid(), taskType, taskId}
	reply := ReportTaskReply{}
	succ := call("Coordinator.ReportTaskDone", &args, &reply)

	return reply.CanExit, succ
}

func checkError(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v)
	}
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
