package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "io"

type AssignmentState int
const (
	Idle AssignmentState = iota
	InProgress
	Completed
)

type TaskStatus struct {
	state AssignmentState
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu    sync.Mutex

	taskPhase TaskPhase
	nReduce int

	filesStatus map[string]*TaskStatus      // 文件对应的状态
	reduceTaskStatus map[int]*TaskStatus // reduce 的输出结果，后续下盘
}

func (c* Coordinator) Init(nReduce int, files []string) {
	c.taskPhase = MapPhase
	c.nReduce = nReduce
	c.filesStatus = make(map[string]*TaskStatus)
	c.reduceTaskStatus = make(map[int]*TaskStatus)
	for _, file := range files {
		c.filesStatus[file] = &TaskStatus{
			state: Idle,
		}
	}
	for id := 0; id < c.nReduce; id++ {
		c.reduceTaskStatus[id]= &TaskStatus{
			state : Idle,
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if (c.taskPhase == MapPhase) {
		complete := true
		for filename, fileStatus := range c.filesStatus {
			if (fileStatus.state != Completed) {
				complete = false;
			}
			if (fileStatus.state == Idle || (fileStatus.state == InProgress && time.Since(fileStatus.startTime) > 10*time.Second)) {
				reply.Filename = filename
				break;
			}
		}
		if complete {
			c.taskPhase = ReducePhase
		} else {
			if (len(reply.Filename) == 0) {
				reply.TaskPhase = WaitPhase
			} else {
				c.filesStatus[reply.Filename].state = InProgress
				c.filesStatus[reply.Filename].startTime = time.Now()
			}
		}
	}
	if (c.taskPhase == ReducePhase) {
		complete := true
		reply.ReduceTaskID = -1
		for taskId, taskStatus := range c.reduceTaskStatus {
			if (taskStatus.state != Completed) {
				complete = false
			}
			if (taskStatus.state == Idle || (taskStatus.state == InProgress && time.Since(taskStatus.startTime) > 10*time.Second)) {
				reply.ReduceTaskID = taskId 
				break;
			}
		}
		if complete {
			c.taskPhase = DonePhase
		} else {
			if (reply.ReduceTaskID == -1) {
				reply.TaskPhase = WaitPhase
			} else {
				c.reduceTaskStatus[reply.ReduceTaskID].state = InProgress
				c.reduceTaskStatus[reply.ReduceTaskID].startTime = time.Now()
			}
		}
	}
	if (reply.TaskPhase != WaitPhase) {
		reply.TaskPhase = c.taskPhase
	}
	reply.NReduce = c.nReduce
	log.Printf("get task: %v", reply)
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if (args.TaskPhase == MapPhase) {
		c.filesStatus[args.Filename].state = Completed
	}
	if (args.TaskPhase == ReducePhase) {
		c.reduceTaskStatus[args.ReduceTaskID].state = Completed
	}
	log.Printf("task completed:%v", args)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := c.taskPhase == DonePhase

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	log.SetOutput(io.Discard)
	c := Coordinator{}
	log.Printf("files:%v", files)
	c.Init(nReduce, files);

	// Your code here.


	c.server(sockname)
	return &c
}
