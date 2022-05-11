package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type TaskResult int

const (
	TaskSuccess TaskResult = 0
	TaskFail    TaskResult = 1
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	id          int
	terminating bool
	mapf        func(string, string) []KeyValue
	reducef     func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	w := worker{
		mapf:        mapf,
		reducef:     reducef,
		terminating: false,
	}

	w.register()
	w.run()
}
func (w *worker) run() {
	log.Printf("Worker %v start running", w.id)
	for !w.terminating {
		t := w.askForTask()
		if t.Stat != TaskExecuting {
			log.Printf("This is not a valid task!")
			break
		}
		w.doTask(t)
	}
	log.Printf("Worker %v terminating....", w.id)
}

/*communications*/
//called for registering worker to the coordinator
func (w *worker) register() {
	args := &RegArgs{}
	reply := &RegReply{}
	if ok := call("Coordinator.RegisterWorker", &args, &reply); !ok {
		log.Fatal("Fail to register!")
	}
	w.id = reply.WorkerId
	log.Printf("Worker %v registered", w.id)
}

func (w *worker) askForTask() Task {
	DPrintf("[w.askForTask]: ask for a new task")
	args := &TaskReqArgs{WorkerId: w.id}
	reply := &TaskReqReply{}
	if ok := call("Coordinator.AssignTask", &args, &reply); !ok {
		log.Fatal("Fail to get a job!")
		os.Exit(1)
	}
	log.Printf("Get task %v", reply.NewTask.TaskId)
	return *reply.NewTask
}

//get called when task is finished or run into error
func (w *worker) reportTask(t Task, result TaskResult, err error) {
	args := TaskReportArgs{
		Complete: true,
		WorkerId: w.id,
		TaskId:   t.TaskId,
	}
	reply := TaskReportReply{}

	if result != TaskSuccess {
		log.Printf("%v", err)
		args.Complete = false
	}
	log.Printf("Report task %v, success? %v", t.TaskId, args.Complete)
	call("Coordinator.ReportTask", &args, &reply)
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

/*do functions*/
func (w *worker) doTask(t Task) {
	switch t.Stage {
	case StageMap:
		w.doMapTask(t)
	case StageRed:
		w.doRedTask(t)
	case StageFinishing:
		w.doFinishingTask(t)
	default:
		log.Fatalf("Something goes wrong, the stage should be either Map or reduce, but now is %v", t.Stage)
	}
}

func (w *worker) doMapTask(t Task) {
	DPrintf("[w.doMapTask] start to do map task")

	contents, err := ioutil.ReadFile(t.FileSrc)
	if err != nil {
		w.reportTask(t, TaskFail, err)
		return
	}

	kvs := w.mapf(t.FileSrc, string(contents))

	//groupify kv to its hash index
	intMedArr := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		hashIdx := ihash(kv.Key) % t.NReduce
		intMedArr[hashIdx] = append(intMedArr[hashIdx], kv)
	}

	//save grouped kvs to the certain file
	for hashKey := 0; hashKey < t.NReduce; hashKey++ {
		intMedFilename := combineName(t.TaskId, hashKey)
		if err := saveKVsToFile(intMedArr[hashKey], intMedFilename); err != nil {
			w.reportTask(t, TaskFail, err)
			return
		}
	}

	w.reportTask(t, TaskSuccess, nil)
}

func (w *worker) doRedTask(t Task) {
	w.reportTask(t, TaskSuccess, nil)
}

func (w *worker) doFinishingTask(t Task) {
	w.terminating = true
	w.reportTask(t, TaskSuccess, nil)
}
