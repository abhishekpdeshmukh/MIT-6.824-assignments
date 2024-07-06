package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

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

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := RequestTask()
		// fmt.Println("mr-" + strconv.Itoa(reply.currMapIndex) + "-" + strconv.Itoa(reply.currReduceIndex))
		if reply.Type == "Map" {
			intermediate := []KeyValue{}
			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("cannot open %v", reply.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File)
			}
			file.Close()

			kva := mapf(reply.File, string(content))
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))

			for _, kv := range intermediate {
				fileName := "mr-" + strconv.Itoa(reply.currMapIndex) + "-" + strconv.Itoa(ihash(kv.Key)%10)
				file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
				enc := json.NewEncoder(file)
				if err != nil {
					log.Fatalf("cannot create file %v: %v", fileName, err)
				}
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode kv %v: %v", kv, err)
				}
				// fmt.Println("Wrote " + fileName)
				file.Close()
			}
			// fmt.Println("Wrote " + fileName)

		} else if reply.Type == "Reduce" {
			fmt.Printf("LOL\n")

			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("Cannot Open  %v in Reduce", reply.File)
			}
			var kvs []KeyValue
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvs = append(kvs, kv)
			}
			i := 0
			fileName := "mr-out-" + strconv.Itoa(reply.TaskNum)
			file, err = os.Create(fileName)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
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
				fmt.Fprintf(file, "%v %v\n", kvs[i].Key, output)

				i = j
			}
			fmt.Println("Data written to file successfully.")
			// break
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func RequestTask() TaskOrderReply {
	args := ExampleArgs{}
	reply := TaskOrderReply{}
	ok := call("Coordinator.AskForTask", &args, &reply)
	if ok {
		// fmt.Printf("Task Type  %v\n", reply.Type)
		// fmt.Printf("Task File Name %v\n", reply.File)
		// fmt.Printf("Task No  %v\n", reply.TaskNum)
		// return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
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
