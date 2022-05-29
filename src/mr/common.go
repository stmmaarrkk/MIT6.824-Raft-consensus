package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	FileSrc  string //used in map stage only
	Stage    ExecStage
	ValidThr time.Time
	NReduce  int //relevant to hash function of the key, equals to nReduce
	NMap     int //number of files in the map stage
}

const Debug = false

func DInit() {
	if Debug {
		files, err := filepath.Glob("mr-*")
		if err != nil {
			panic(err)
		}
		for _, f := range files {
			if err := os.Remove(f); err != nil {
				panic(err)
			}
		}
	}
}
func DPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format+"\n", v...)
	}
}

func combineName(taskId int, hashKey int) string {
	return fmt.Sprintf("mr-%v-%v.json", taskId, hashKey)
}

//modify the kvMap inplace
func readKVsFromFile(fileSrc string, kvMap map[string][]string) error {
	file, err := os.Open(fileSrc)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			//break when eof happens
			break
		}

		//initiate this key
		if _, ok := kvMap[kv.Key]; !ok {
			kvMap[kv.Key] = make([]string, 0, 100)
		}
		kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	}

	return nil
}

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
