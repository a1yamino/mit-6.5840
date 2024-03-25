package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	rep := &AssignReply{}
	call("Coordinator.Assign", &AssignArgs{}, rep)
	switch rep.Tasktype {
	case TaskMap:
		executeMap(mapf, rep.Filename, rep.NReduces)
	case TaskReduce:
		executeReduce(reducef)
	}
	// Your worker implementation here.

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				// Ticker的C通道在每个间隔时间发送一个时间值
				// 当时间到达时，发送心跳
				SendHeartbeat()
			}
		}
	}()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
		fmt.Printf("call failed!\n")
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

func SendHeartbeat() {
	call("Coordinator.Heartbeat", &HeartbeatArgs{}, &HeartbeatReply{})
}

func executeMap(mapf func(string, string) []KeyValue, inputFile string, nReduce int) {
	file, _ := os.Open(inputFile)
	contents, _ := io.ReadAll(file)
	kva := mapf(inputFile, string(contents))

	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-", mapTaskNumber, i))
		if err != nil {
			log.Fatalf("cannot create temp file for bucket %d", i)
		}

		defer tempFile.Close()
		encoders[i] = json.NewEncoder(tempFile)
	}

	// 将键值对分配到桶中
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		err := encoders[bucket].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write to temp file for bucket %d", bucket)
		}
	}
}

func executeReduce(reducef func(string, []string) string) {

}
