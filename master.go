package mapreduce

type Master struct {
}

//任务调度的函数
func Sequential(jobName string,
	files []string,
	nReduce int,
	mapFunc func(file string, value string) []KeyValue,
	reduceFunc func(key string, value []string) string) {

	//执行分配的任务
	m := newMaster()
	m.run(jobName, files, nReduce, func(phase jobPhase) {
		switch phase {
		case MapPhase:
			for index, v := range files {
				doMap(jobName, index, v, nReduce, mapFunc)
			}
		case ReducePhase:
			for i := 0; i < nReduce; i++ {
				DoReduce(jobName, i, len(files), MergeName(jobName, i), reduceFunc)
			}
		}
	})
}

func newMaster() *Master {
	return nil
}

//执行
func (m *Master) run(jobName string, files []string, nReduce int, schedule func(phase jobPhase)) {
	schedule(MapPhase)
	schedule(ReducePhase)
}
