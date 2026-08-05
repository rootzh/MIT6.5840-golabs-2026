package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "path/filepath"
import "time"
import "sort"
import "io"


// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

func writeFileForMap(filename string, nReduce int, kva []KeyValue) error {
	kvSplit := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		kvSplit[idx] = append(kvSplit[idx], kv)
		// log.Printf("file:%s, key:%s, idx:%d", filename, kv.Key, idx)
	}
	dir, _ := os.Getwd()
	for idx, kvList := range kvSplit {
		tmpFile, err := os.CreateTemp(dir, "map-out-tmp-*")
		if err != nil {
			log.Printf("create file fail, idx:%d, err:%s", idx, err)
			return err
		}
		enc := json.NewEncoder(tmpFile)
		err = enc.Encode(&kvList)
		if err != nil {
			log.Printf("encode json fail, err:%s", err)
			tmpFile.Close()
			return err
		}
		tmpFile.Close()
		outFilename := fmt.Sprintf("map-out-%s-%d", filepath.Base(filename), idx)
		// log.Printf("outFile:%s, idx:%d, len:%d", outFilename, idx, len(kvList))
		if err := os.Rename(tmpFile.Name(), outFilename); err != nil {
			return err
		}
	}
	return nil
}

func readKeyValueFromFile(reduceTaskID int) (error, []KeyValue) {
	pattern := fmt.Sprintf("map-out-*-%d", reduceTaskID)
	files, _ := filepath.Glob(pattern)

	var allkvs []KeyValue
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			log.Printf("open file fail: filename:%s, err:%s", f, err)
			return err, make([]KeyValue, 0)
		}
		dec := json.NewDecoder(file)
		var kvs []KeyValue
		if err := dec.Decode(&kvs); err != nil {
			file.Close()
			log.Printf("decode fail filename:%s, err:%s", f, err)
			return err, make([]KeyValue, 0)
		}
		file.Close()
		allkvs = append(allkvs, kvs...)
	}
	return nil, allkvs
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetOutput(io.Discard)

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		ok, taskInfo := CallGetTask()
		if !ok {
			log.Printf("get task fail")
			return
		}
		log.Printf("get task phase:%d, filename:%s, reduceTaskID:%d", taskInfo.TaskPhase, taskInfo.Filename, taskInfo.ReduceTaskID)
		if (taskInfo.TaskPhase == WaitPhase) {
			time.Sleep(3 * time.Second)
			continue;
		}
		if (taskInfo.TaskPhase == DonePhase) {
			break;
		}
		taskCompleteArgs := TaskCompleteArgs{}
		if (taskInfo.TaskPhase == MapPhase) {
			content := readFile(taskInfo.Filename)
			kva := mapf(taskInfo.Filename, content)
			err := writeFileForMap(taskInfo.Filename, taskInfo.NReduce, kva)
			if err != nil {
				log.Printf("map write fail, filename:%s, err:%s", taskInfo.Filename, err)
				continue
			}

			taskCompleteArgs.TaskPhase = taskInfo.TaskPhase
			taskCompleteArgs.Filename = taskInfo.Filename
		}
		if (taskInfo.TaskPhase == ReducePhase) {
			err, kvs := readKeyValueFromFile(taskInfo.ReduceTaskID)
			if (err != nil) {
				log.Printf("reduce read file fail, reduceTaskID:%d, err:%v", taskInfo.ReduceTaskID, err)
				continue
			}
			dir, _ := os.Getwd()
			tmpFile, err := os.CreateTemp(dir, "mr-out-tmp-*")
			outFilename := fmt.Sprintf("mr-out-%d", taskInfo.ReduceTaskID)

			sort.Sort(ByKey(kvs))
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
				fmt.Fprintf(tmpFile, "%v %v\n", kvs[i].Key, output)

				i = j
			}
			tmpFile.Close()
			err = os.Rename(tmpFile.Name(), outFilename)
			if err != nil {
				log.Printf("rename file fail, file:%s, tmp file name:%s, err:%s", outFilename, tmpFile.Name(), err)
				continue
			}
			taskCompleteArgs.TaskPhase = taskInfo.TaskPhase
			taskCompleteArgs.ReduceTaskID = taskInfo.ReduceTaskID
		}
		ok = CallTaskComplete(taskCompleteArgs)
		if !ok {
			log.Printf("call task Complete fail:%d, filename:%s, reduceTaskID:%d", taskInfo.TaskPhase, taskInfo.Filename, taskInfo.ReduceTaskID)
		}

	}

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

func CallGetTask() (bool, GetTaskReply) {
	args := GetTaskArgs{}

	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	return ok, reply
}

func CallTaskComplete(args TaskCompleteArgs) bool {
	reply := TaskCompleteReply{}

	return call("Coordinator.TaskComplete", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
