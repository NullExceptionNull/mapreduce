package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

//具体Reduce 函数
func DoReduce(jobName string,
	reduceTaskNum int,
	mapTaskId int,
	outFile string,
	reduceFunc func(key string, value []string) string) {

	var ret = make(map[string][]string)

	for i := 0; i < mapTaskId; i++ {
		fileName := ReduceName(jobName, i, reduceTaskNum)
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("open file [%s] %v", fileName, err)
		}
		defer f.Close()

		kv := KeyValue{}

		d := json.NewDecoder(f)

		for d.More() {
			err2 := d.Decode(&kv)
			if err2 != nil {
				log.Fatalf("Decode json [%s] %v", fileName, err)
			}
			ret[kv.Key] = append(ret[kv.Key], kv.Value)
		}

		var keys []string

		for key := range ret {
			keys = append(keys, key)
		}

		f2, err2 := os.Create(outFile)

		if err2 != nil {
			log.Fatalf("outFile Create [%s] %v", fileName, err2)
		}

		defer f2.Close()

		encode := json.NewEncoder(f2)

		for _, v := range keys {
			encode.Encode(KeyValue{v, reduceFunc(v, ret[v])})
		}
	}
}
