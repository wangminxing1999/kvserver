package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		time.Sleep(time.Millisecond * 100)
		args := Args{}
		reply := Reply{}
		call("Master.TaskAssign", &args, &reply)
		switch reply.TaskType {
		case maptask:
			if reply.Title == "" {
				return
			}
			sort_kind := make(map[int][]KeyValue)
			file, err := os.Open(reply.Title)
			if err != nil {
				log.Fatalf("cannot 1open %v", reply.Title)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Title)
			}
			file.Close()
			temp_store := mapf(reply.Title, string(content))
			for _, k := range temp_store {
				sort_kind[ihash(k.Key)%reply.NReduce] = append(sort_kind[ihash(k.Key)%reply.NReduce], k)
			}
			path, _ := os.Getwd()
			for i := 0; i < reply.NReduce; i++ {
				s := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
				temp_file, err := ioutil.TempFile(path, "*")
				if err != nil {
					log.Fatalf("cannot build temparory files")
				}
				enc := json.NewEncoder(temp_file)
				for _, j := range sort_kind[i] {
					err := enc.Encode(&j)
					if err != nil {
						log.Fatalf("cannot encode")
					}
				}
				os.Rename(temp_file.Name(), s)
				ra := ReportArgs{}
				rp := ReportReply{}
				ra.TaskID = reply.TaskID
				ra.TaskType = reply.TaskType
				call("Master.ReportTask", &ra, &rp)
			}
		case reducetask:
			intermediate := []KeyValue{}
			id := reply.TaskID
			num := reply.MapTaskNum
			for i := 0; i < num; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, id)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			path, _ := os.Getwd()
			temp_file, err := ioutil.TempFile(path, "*")

			if err != nil {
				log.Fatalf("cannot build temparory files")
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(temp_file, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			wanted_name := fmt.Sprintf("mr-out-%d", id)
			os.Rename(temp_file.Name(), wanted_name)
			ra := ReportArgs{}
			rp := ReportReply{}
			ra.TaskID = reply.TaskID
			ra.TaskType = reply.TaskType
			call("Master.ReportTask", &ra, &rp)
		case wait:
			continue
		case end:
			return
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
