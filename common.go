package mapreduce

import "strconv"

type jobPhase string

const (
	MapPhase    jobPhase = "Map"
	ReducePhase          = "Reduce"
)

type KeyValue struct {
	Key   string
	Value string
}

func MergeName(jobName string, reduceTask int) string {
	return "tmp" + "-" + jobName + strconv.Itoa(reduceTask)
}

func ReduceName(jobName string, mapTaskId int, reduceTaskId int) string {
	return "tmp" + "-" + jobName + strconv.Itoa(mapTaskId) + "-" + strconv.Itoa(reduceTaskId)
}
