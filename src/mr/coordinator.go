package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	SchedulePeriod = time.Millisecond * 500
	TaskDuration   = time.Second * 5
)

type Coordinator struct {
	files   []string
	nReduce int
	nFiles  int //number of files to be mapped
	stage   ExecStage

	taskCompCount int //count the number of completed tasks

	tasks     []Task
	taskQueue chan int //stores the id of tasks in queue

	nWorkers int
	lock     sync.Mutex
}

/* RPC handlers */
func (c *Coordinator) RegisterWorker(args *RegArgs, reply *RegReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.stage == StageFinishing || c.stage == StageComplete {
		return errors.New("the coordinator is terminating, cannot register new worker")
	} else {
		reply.WorkerId = c.nWorkers
		c.nWorkers++
		log.Printf("A worker registered, there are %v workers now.\n", c.nWorkers)
		return nil
	}
}

func (c *Coordinator) AssignTask(args *TaskReqArgs, reply *TaskReqReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	DPrintf("[c.AssignTask]: Start to assign a task")
	c.scheduleTasks(false)
	taskIdAssigned := <-c.taskQueue
	c.activateTask(taskIdAssigned, args.WorkerId)
	newTask := c.tasks[taskIdAssigned]
	reply.NewTask = &newTask
	log.Printf("Task %v is assigned, %v tasks remain in queue", taskIdAssigned, len(c.taskQueue))
	return nil
}

func (c *Coordinator) ReportTask(args *TaskReportArgs, reply *TaskReportReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	DPrintf("[c.ReportTask]: Worker %v report task %v", args.WorkerId, args.TaskId)
	//ignore the report if the task is expired.
	if c.validateReport(args) {
		c.tasks[args.TaskId].Stat = TaskComplete
		c.taskCompCount++
		log.Printf("Task %v success! (%v/%v tasks completed)", args.TaskId, c.taskCompCount, len(c.tasks))
	} else {
		c.tasks[args.TaskId].Stat = TaskTerminated
		log.Printf("Worker %v fail to complete task %v", args.WorkerId, args.TaskId)
	}
	return nil
}

// New thread for listening for RPCs from worker.go
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

/* for Map stage */
func (c *Coordinator) initMapStage() {
	c.stage = StageMap
	c.prepareMapTasks()
	log.Printf("Switch to Map Stage")
}

func (c *Coordinator) prepareMapTasks() {
	c.taskCompCount = 0
	c.taskQueue = make(chan int, c.nFiles)
	c.tasks = make([]Task, c.nFiles)
	for i := range c.files {
		c.initMapTask(i)
	}
	log.Printf("%v map tasks are created", len(c.tasks))
}

func (c *Coordinator) initMapTask(taskId int) {
	//modify task inplace
	c.tasks[taskId] = Task{
		WorkerId: -1,
		TaskId:   taskId,
		Stat:     TaskReady,
		FileSrc:  c.files[taskId],
		Stage:    c.stage,
		NReduce:  c.nReduce,
	}
}

/* For reduce stage */
func (c *Coordinator) initRedStage() {
	c.stage = StageRed
	c.prepareRedTasks()
	log.Printf("Switch to Reduce Stage")
}

func (c *Coordinator) prepareRedTasks() {
	c.tasks = make([]Task, c.nReduce)
	c.taskQueue = make(chan int, c.nReduce)
	c.taskCompCount = 0
	// for i := range files {
	// 	c.initMapTask(i)
	// }
	// log.Printf("%v map tasks are created", len(c.tasks))
}

func (c *Coordinator) initRedTask(taskId int) {
	//modify task inplace
	c.tasks[taskId] = Task{
		WorkerId: -1,
		TaskId:   taskId,
		Stat:     TaskReady,
		FileSrc:  c.files[taskId],
		Stage:    c.stage,
		NReduce:  c.nReduce,
	}
}

/* Finishing stage*/
func (c *Coordinator) initFinishingStage() {
	c.stage = StageFinishing
	c.prepareFinishingTasks()
	log.Printf("Switch to Finishing Stage")
}

func (c *Coordinator) prepareFinishingTasks() {
	c.taskCompCount = 0
	c.taskQueue = make(chan int, c.nWorkers)
	c.tasks = make([]Task, c.nWorkers)
	for i := 0; i < c.nWorkers; i++ {
		c.initFinishingTask(i)
	}
	log.Printf("%v finishing tasks are created", len(c.tasks))
}

func (c *Coordinator) initFinishingTask(workerId int) {
	//modify task inplace
	c.tasks[workerId] = Task{
		WorkerId: -1,
		TaskId:   workerId,
		Stat:     TaskReady,
		FileSrc:  "",
		Stage:    c.stage,
		NReduce:  c.nReduce,
	}
}

/* general functions (for both stage)*/
func (c *Coordinator) initiate(nReduce int, files []string) {
	log.Printf("Coordinator initiated")
	c.nReduce = nReduce
	c.files = files
	c.nFiles = len(files)
}

func (c *Coordinator) run() {
	for !c.Done() {
		go c.scheduleTasks(true)
		time.Sleep(SchedulePeriod)
	}
}

func (c *Coordinator) scheduleTasks(needLock bool) {
	if needLock {
		c.lock.Lock()
		defer c.lock.Unlock()
	}

	DPrintf("[c.scheduleTasks]: Scheduling task")
	c.toNextStageIfNecessary()

	//the following snipet might do nothing if all tasks are complete
	for taskId, task := range c.tasks {
		// DPrintf("[c.scheduleTasks]: Task %v in state: %v", taskId, task.Stat)
		switch task.Stat {
		case TaskReady:
			c.addToQueue(taskId)
		case TaskExecuting:
			if time.Now().Sub(c.tasks[taskId].ValidThr) > 0 {
				c.initTask(taskId)
				DPrintf("Task %v has been reset due to expiration", taskId)
				c.addToQueue(taskId) //must add it back to the queue immediately or deadlock takes place at the last task(since one more cycle is needed to add it back)
			}
		case TaskComplete:
		case TaskTerminated:
			c.initTask(taskId)
			DPrintf("Task %v has been reset due to fail execution", taskId)
			c.addToQueue(taskId)
		default:
		}

	}
}

func (c *Coordinator) initTask(taskId int) {
	switch c.stage {
	case StageMap:
		c.initMapTask(taskId)
		// case StageRed:
		// 	c.initRedTask(taskId)
	}
}

func (c *Coordinator) addToQueue(taskId int) {
	c.tasks[taskId].Stat = TaskQueued

	c.taskQueue <- taskId
	log.Printf("Task %v has been added to task queue, %v tasks remain in queue", taskId, len(c.taskQueue))
}

// func (c *Coordinator) resetExpired(taskId int) {
// 		DPrintf("Task %v has been reset", taskId)
// 	}
// }

func (c *Coordinator) activateTask(taskId int, workerId int) {
	c.tasks[taskId].Stat = TaskExecuting
	c.tasks[taskId].ValidThr = time.Now().Add(TaskDuration)
	c.tasks[taskId].WorkerId = workerId
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	// DPrintf("Done is called")
	d := c.stage == StageComplete
	if d {
		log.Printf("All tasks are done.")
	}
	return d
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("Debug mode:%v", Debug)
	c := Coordinator{stage: StageInit}
	c.initiate(nReduce, files)
	go c.run()
	c.server()
	return &c
}

/*helper*/
func (c *Coordinator) validateReport(args *TaskReportArgs) bool {
	task := c.tasks[args.TaskId]
	if !args.Complete {
		DPrintf("[c.validateReport]: validation failed due to incomplete task")
		return false
	}
	if !(task.WorkerId == args.WorkerId) {
		DPrintf("[c.validateReport]: validation failed due to unmatched worker id")
		return false
	}
	if time.Since(task.ValidThr) > 0 {
		DPrintf("[c.validateReport]: validation failed due to task expiration")
		return false
	}
	return true
}

//return true if stage transition is needed
func (c *Coordinator) toNextStageIfNecessary() {
	// DPrintf("[c.toNextStageIfNecessary]: Current stage is:%v", c.stage)
	switch c.stage {
	case StageInit:
		c.initMapStage()
	case StageMap:
		if c.nFiles == c.taskCompCount {
			c.initFinishingStage() //modify this
		}
	case StageRed:
		if c.nReduce == c.taskCompCount {
			c.initFinishingStage()
		}
	case StageFinishing:
		if c.nWorkers == c.taskCompCount {
			c.stage = StageComplete
			log.Printf("To complete stage!")
		}
	default:
		return
	}
}
