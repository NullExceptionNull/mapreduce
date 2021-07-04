package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

//实现一个map 任务管理函数,从inputfiles 读取内容
//将输出分成指定数量的中间文件

func doMap(jobName string, mapTaskNum int, inFile string, reduceNum int, mapF func(file string, contents string) []KeyValue) {
	// 打开文件
	f, err := os.Open(inFile)
	if err != nil {
		log.Fatalf("open file %s error %v \n", inFile, err)
	}
	defer f.Close()

	content, err := ioutil.ReadAll(f)

	if err != nil {
		log.Fatalf("read file %s error %v \n", inFile, err)
	}

	kvs := mapF(inFile, string(content))

	encoders := make([]*json.Encoder, reduceNum)

	for i := 0; i < reduceNum; i++ {
		s := ReduceName(jobName, mapTaskNum, i)
		file, err := os.Create(s)
		if err != nil {
			log.Fatalf("create file [%s] error %v \n", s, err)
		}
		defer file.Close()
		encoders[i] = json.NewEncoder(file)
	}

	for _, v := range kvs {
		index := iHash(v.Key) % reduceNum
		err = encoders[index].Encode(&v)
		if err != nil {
			log.Fatal("write file error \n")
		}
	}

}

func iHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
