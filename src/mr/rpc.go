package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

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
// coordinator 与 worker 之间需要传输什么？ -> Task

type Task struct {
	TaskType  TaskType   //任务类型：Map && Reduce && Exit
	TaskId    int        //任务id
	FileName  string     //文件名
	ReduceNum int        //有几个reducer，用于hash
	MapNum    int        //有几个map任务
	Status    StatusType //当前状态
	StartTime time.Time  //记录当前状态的开始时间
}

type TaskArgs struct{} //worker只是向coordinator获取一个任务，所以不需要传入参

type TaskType int8

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
	WaitTask
)

type StatusType int8

const (
	Waiting StatusType = iota
	Working
	Done
	Fail
)

type ReportReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
