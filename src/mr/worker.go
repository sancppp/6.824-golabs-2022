package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// 人话：通过哈希取模确认这个单词的reduce任务编号
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// CallExample() // for test

	// Your worker implementation here.
	keep := true
	for keep {
		task, err := getTask()
		if err != nil {
			log.Fatalf("get task fail! errinfo:%v", err)
		}
		switch task.TaskType {
		case MapTask:
			{
				log.Printf("Do Map Task.\n")
				DoMapTask(mapf, task)
			}
		case ReduceTask:
			{
				log.Printf("Do Reduce Task.\n")
				DoReduceTask(reducef, task)
			}
		case ExitTask:
			{
				log.Printf("Exit.\n")
				keep = false
				return
			}
		case WaitTask:
			{
				time.Sleep(5 * time.Second)
			}
		}
	}

}

// 向coordinator拿一个任务
func getTask() (*Task, error) {
	taskArgs := TaskArgs{}
	taskReply := Task{}

	ok := call("Coordinator.GetTask", &taskArgs, &taskReply)
	if ok {
		log.Printf("get task success! task:%v\n", taskReply)
	} else {
		log.Printf("call failed!\n")
	}
	return &taskReply, nil
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		task.Status = Fail
		call("Coordinator.Report", &task, &ReportReply{})
		log.Fatalf("cannot open %v", filename)
	}

	alls, err := ioutil.ReadAll(file)
	if err != nil {
		task.Status = Fail
		call("Coordinator.Report", &task, &ReportReply{})
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	task.Status = Working
	temp := mapf(filename, string(alls)) //执行map函数，中间结果拿到
	//将中间结果存储成临时文件
	reducerNum := task.ReduceNum
	HashedKV := make([][]KeyValue, reducerNum)
	for _, kv := range temp {
		HashedKV[ihash(kv.Key)%reducerNum] = append(HashedKV[ihash(kv.Key)%reducerNum], kv)
	}

	for i := 0; i < reducerNum; i++ {
		//create intermediate file
		//mr-X-Y
		filename := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		log.Println("create file : ", filename)
		file, _ := os.Create(filename)
		enc := json.NewEncoder(file)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		file.Close()
	}

	task.Status = Done
	call("Coordinator.Report", &task, &ReportReply{})
}

// DoReduceTask 处理reduce task
func DoReduceTask(reducef func(string, []string) string, task *Task) {

	var intermediate []KeyValue
	//读取中间产物，加载进内存
	for i := 0; i < task.MapNum; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + task.FileName
		file, err := os.Open(filename)
		if err != nil {
			task.Status = Fail
			call("Coordinator.Report", &task, &ReportReply{})
			log.Fatalf("cannot open %v", filename)
			return
		}
		log.Println("open file : ", filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + task.FileName
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	task.Status = Done
	call("Coordinator.Report", &task, &ReportReply{})
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
