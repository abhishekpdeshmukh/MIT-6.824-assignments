package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	inputMapFiles    []string
	inputReduceFiles []string
	finalOutputFiles []string
	currMapIndex     int
	currReduceIndex  int
	numReduceTask    int
	mu               sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) AskForTask(args *ExampleArgs, reply *TaskOrderReply) error {
	// fmt.Printf("REQ")
	c.mu.Lock() // Lock the mutex before accessing shared variables
	defer c.mu.Unlock()
	if c.currMapIndex < len(c.inputMapFiles) {
		reply.Type = "Map"
		// fmt.Printf("currMapIndex %v\n", c.currMapIndex)
		reply.File = c.inputMapFiles[c.currMapIndex]
		reply.TaskNum = c.currMapIndex
		reply.currMapIndex = c.currMapIndex
		reply.currReduceIndex = c.currReduceIndex
		c.currMapIndex = c.currMapIndex + 1
	} else if c.currReduceIndex < len(c.inputReduceFiles) {
		// fmt.Printf("hahahahahaha\n")
		reply.Type = "Reduce"
		// fmt.Printf("currReduceIndex %v\n", c.currReduceIndex)
		reply.File = c.inputReduceFiles[c.currReduceIndex]
		reply.TaskNum = c.currReduceIndex
		reply.currMapIndex = c.currMapIndex
		reply.currReduceIndex = c.currReduceIndex
		c.currReduceIndex = c.currReduceIndex + 1
	}

	// fmt.Println("mr-" + strconv.Itoa(reply.currMapIndex) + "-" + strconv.Itoa(reply.currReduceIndex))
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

	if c.currReduceIndex >= len(c.inputReduceFiles) {
		fmt.Printf("THE MAPREDUCE IS DONE\n")
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

	var finalOutputFiles = []string{}
	var inputReduceFiles = []string{}
	for j := 0; j < nReduce; j++ {
		finalOutputFiles = append(finalOutputFiles, "mr-out-"+strconv.Itoa(j))
	}
	for j := 0; j < nReduce; j++ {
		inputReduceFiles = append(inputReduceFiles, "mr-0-"+strconv.Itoa(j))
	}
	// fmt.Println(len(inputReduceFiles))
	// fmt.Println(inputReduceFiles[0])

	c := Coordinator{
		inputMapFiles:    files,
		inputReduceFiles: inputReduceFiles,
		finalOutputFiles: finalOutputFiles,
		currReduceIndex:  0,
		currMapIndex:     0,
		numReduceTask:    nReduce,
	}

	// Your code here.

	c.server()
	return &c
}
