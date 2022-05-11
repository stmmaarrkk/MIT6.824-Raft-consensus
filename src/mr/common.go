package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

//Execution Stage
type ExecStage int

const (
	StageInit      ExecStage = 0
	StageMap       ExecStage = 1
	StageRed       ExecStage = 2
	StageFinishing ExecStage = 3 //Job done, but not yet terminating the workers
	StageComplete  ExecStage = 4 //all job done, *plus all cleanup jobs are executed
)

//status of each task
type TaskStat int

const (
	TaskReady      TaskStat = 0 //just prepared, but is not yet in queue
	TaskQueued     TaskStat = 1 //stay in the queue, but is not yet selected
	TaskExecuting  TaskStat = 2
	TaskComplete   TaskStat = 3
	TaskTerminated TaskStat = 4
)

//since task will be copy, there can only be primitive types in fields
type Task struct {
	WorkerId int
	TaskId   int
	Stat     TaskStat
	FileSrc  string
	Stage    ExecStage
	ValidThr time.Time
	NReduce  int //relevant to hash function of the key, equals to nReduce
}

const Debug = false

// func DInit(){
// 	for
// }
func DPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format+"\n", v...)
	}
}

func combineName(taskId int, hashKey int) string {
	return fmt.Sprintf("mr-%v-%v", taskId, hashKey)
}

// func readKVsFromFile(fileSrc string) ([]KeyValue, error) {
// 	var kvs []KeyValue

// 	file, err := os.Open(fileSrc)
// 	if err != nil {
// 		return kvs, err
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	// optionally, resize scanner's capacity for lines over 64K, see next example
// 	for scanner.Scan() {
// 		kv := strings.Split(scanner.Text(), " ")
// 		kvs = append(kvs, KeyValue{kv[0], kv[1]})
// 	}

// 	if err := scanner.Err(); err != nil {
// 		return kvs, err
// 	}

// 	return kvs, nil
// }

func saveKVsToFile(kvs []KeyValue, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		err := enc.Encode(&kv)

		if err != nil {
			return err
		}
	}
	return nil
}
