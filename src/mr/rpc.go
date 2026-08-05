package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskPhase int

const (
    MapPhase    TaskPhase = iota  // 0
    ReducePhase                   // 1
    DonePhase                     // 2
	WaitPhase
)


type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskPhase TaskPhase
	Filename string   // map task use
	NReduce int

	// reduce task use
	ReduceTaskID int
}

type TaskCompleteArgs struct {
	TaskPhase TaskPhase
	Filename string

	ReduceTaskID int
}

type TaskCompleteReply struct {
}
