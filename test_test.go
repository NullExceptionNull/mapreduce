package mapreduce

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
)

const NUM = 10

//创建一个包含N个编号的输入文件
//通过MR进行处理
//检查最终输出文件中是否包含了N个编号
// 自定义Map 分割处理函数
func MapFunc(file string, value string) (res []KeyValue) {
	words := strings.Fields(value)
	for _, v := range words {
		kv := KeyValue{Key: v, Value: ""}
		res = append(res, kv)
	}
	return res
}

func TestMapFunc(t *testing.T) {
	MapFunc("aa", "helloWorld! Nihao")
}

//自定义 Reduce 聚合函数
func ReduceFunc(key string, values []string) string {

	for _, item := range values {
		fmt.Printf("Reduce %s-%v\n", key, item)
	}

	return ""
}

func TestSequentialSingle(t *testing.T) {
	Sequential("MR1", makeInputs(1), 1, MapFunc, ReduceFunc)
}

// func TestSequentialMulti(t *testing.T) {
// 	Sequential("MR1", makeInputs(5), 1, MapFunc, ReduceFunc)
// }

func makeInputs(count int) []string {
	var names []string

	var j = 0

	for i := 0; i < count; i++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", i))
		f, err := os.Create(names[i])
		if err != nil {
			log.Fatalf("Create input file [%d] failed error %v", i, err)
		}

		defer f.Close()

		w := bufio.NewWriter(f)

		for j < (i+1)*(NUM/count) {
			fmt.Fprintf(w, "%d\n", j)
			j++
		}
		w.Flush()
	}
	return names
}
