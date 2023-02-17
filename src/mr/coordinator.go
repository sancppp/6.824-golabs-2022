package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Progress int8

const (
	MapP Progress = iota
	ReduceP
	FinishP
)

/*
设计一个调度器
功能：
1. 向worker发布task（管理task）
2. 控制工作是否done
3. 如果一个任务10s还没完成，则换一个worker
*/
type Coordinator struct {
	// Your definitions here.
	ReduceNum         int           //the number of reduce tasks 输入参数决定
	MapNum            int           //the number of map tasks
	FinishedMap       int           //已完成map
	TaskId            int           //当前的Task编号
	ReduceTaskChannel chan *Task    // 使用chan保证并发安全
	MapTaskChannel    chan *Task    // 使用chan保证并发安全
	files             []string      //文件名数组
	TaskId2Task       map[int]*Task //反向映射
	Progress          Progress      //进度
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// GetTask 向worker返回一个task 调度器
func (c *Coordinator) GetTask(args *TaskArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//先把map task发完，再发reduce task,最后发退出信号
	if len(c.MapTaskChannel) > 0 {
		*reply = *<-c.MapTaskChannel
		c.TaskId2Task[reply.TaskId].Status = Working
		c.TaskId2Task[reply.TaskId].StartTime = time.Now()
	} else if c.Progress == ReduceP && len(c.ReduceTaskChannel) > 0 {
		*reply = *<-c.ReduceTaskChannel
		c.TaskId2Task[reply.TaskId].Status = Working
		c.TaskId2Task[reply.TaskId].StartTime = time.Now()
	} else if c.Progress == FinishP {
		reply.TaskType = ExitTask
	} else {
		reply.TaskType = WaitTask
	}
	return nil
}

// Report 用于worker完成一个工作后向coordinator汇报
func (c *Coordinator) Report(args *Task, reply *ReportReply) error {
	//log.Println("get report : ", args)
	if args.Status == Done {
		c.mu.Lock()
		c.TaskId2Task[args.TaskId].Status = Done
		c.TaskId2Task[args.TaskId].StartTime = time.Now()
		if args.TaskType == MapTask {
			c.FinishedMap++
			if c.FinishedMap == c.MapNum {
				c.Progress = ReduceP
			}
		}
		c.mu.Unlock()
	} else if args.Status == Fail {
		//失败了，重新回到队列
		args.Status = Waiting
		c.TaskId2Task[args.TaskId].Status = Waiting
		c.TaskId2Task[args.TaskId].StartTime = time.Now()
		if args.TaskType == MapTask {
			c.MapTaskChannel <- args
		} else if args.TaskType == ReduceTask {
			c.ReduceTaskChannel <- args
		}
	}
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
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		//log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.Progress == FinishP {
		return true
	}
	ret := true

	// Your code here.
	for _, task := range c.TaskId2Task {
		if task.Status != Done {
			ret = false
			break
		}
	}
	if ret {
		c.Progress = FinishP
	}
	return ret
}

func (c *Coordinator) Init() {
	//创建map tasks
	for _, file := range c.files {
		task := Task{
			TaskType:  MapTask,
			TaskId:    c.TaskId,
			FileName:  file,
			ReduceNum: c.ReduceNum,
			Status:    Waiting,
			StartTime: time.Now(),
		}
		log.Println("make a map task :", &task)
		c.TaskId2Task[task.TaskId] = &task
		c.MapTaskChannel <- &task
		c.TaskId++
	}
	c.MapNum = c.TaskId

	//创建reduce tasks
	for i := 0; i < c.ReduceNum; i++ {
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    c.TaskId,
			FileName:  strconv.Itoa(i),
			ReduceNum: c.ReduceNum,
			MapNum:    c.MapNum,
			Status:    Waiting,
			StartTime: time.Now(),
		}
		log.Println("make a reduce task :", &task)
		c.TaskId2Task[task.TaskId] = &task
		c.ReduceTaskChannel <- &task
		c.TaskId++
	}
}

// 任务超时（10s）则转移
func (c *Coordinator) CrashDetector() {
	for _ = range time.Tick(5 * time.Second) {
		c.mu.Lock()
		if c.Progress == FinishP {
			c.mu.Unlock()
			break
		}
		for _, task := range c.TaskId2Task {
			if task.Status == Working {
				log.Printf("%v is working.\n", task)
				if time.Since(task.StartTime) > 10*time.Second {
					log.Printf("%v time out.\n", task)
					c.TaskId2Task[task.TaskId].Status = Waiting
					c.TaskId2Task[task.TaskId].StartTime = time.Now()
					switch task.TaskType {
					case MapTask:
						c.MapTaskChannel <- task
					case ReduceTask:
						c.ReduceTaskChannel <- task
					}
				}
			}

		}
		c.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReduceNum:         nReduce,
		MapNum:            0,
		TaskId:            0,
		ReduceTaskChannel: make(chan *Task, nReduce),
		MapTaskChannel:    make(chan *Task, len(files)),
		files:             files,
		TaskId2Task:       make(map[int]*Task, len(files)+nReduce),
		Progress:          MapP,
	}
	// Your code here.
	c.Init()
	c.server()
	go c.CrashDetector()
	return &c
}
