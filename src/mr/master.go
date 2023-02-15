package mr

import (
	"log"
	"os"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

const (
	notassigned = iota
	doing
	finished
	MAP_PHASE
	REDUCE_PHASE
	END
)

type MapTask struct {
	Id        int
	Title     string
	State     int
	StartTime time.Time
}

type ReduceTask struct {
	Id        int
	State     int
	StartTime time.Time
}

type Master struct {
	// Your definitions here.
	MapTaskQ    []MapTask
	ReduceTaskQ []ReduceTask
	MapTaskNum  int
	Phase       int
	NReduce     int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) ReportTask(args *ReportArgs, reply *ReportReply) error {
	switch args.TaskType {
	case maptask:
		mutex.Lock()
		m.MapTaskQ[args.TaskID].State = finished
		for _, t := range m.MapTaskQ {
			if t.State != finished {
				mutex.Unlock()
				return nil
			}
		}
		m.Phase = REDUCE_PHASE
		mutex.Unlock()
	case reducetask:
		mutex.Lock()
		m.ReduceTaskQ[args.TaskID].State = finished
		for _, t := range m.ReduceTaskQ {
			if t.State != finished {
				mutex.Unlock()
				return nil
			}
		}
		m.Phase = END
		mutex.Unlock()
	}
	return nil
}

func (m *Master) TaskAssign(args *Args, reply *Reply) error {
	mutex.Lock()
	switch m.Phase {
	case MAP_PHASE:
		for i, t := range m.MapTaskQ {
			if t.State == doing && time.Now().Sub(t.StartTime) >= time.Second*10 {
				m.MapTaskQ[i].State = notassigned
			}
			reply.TaskType = wait
			if t.State == notassigned && t.Title != "" {
				reply.MapTaskNum = m.MapTaskNum
				reply.TaskID = t.Id
				reply.Title = t.Title
				reply.NReduce = m.NReduce
				reply.TaskType = maptask
				m.MapTaskQ[i].State = doing
				m.MapTaskQ[i].StartTime = time.Now()
				break
			}
		}

	case REDUCE_PHASE:
		for i, t := range m.ReduceTaskQ {
			if t.State == doing && time.Now().Sub(t.StartTime) >= time.Second*10 {
				m.ReduceTaskQ[i].State = notassigned
			}
			if t.State == notassigned {
				reply.TaskID = t.Id
				reply.MapTaskNum = m.MapTaskNum
				reply.NReduce = m.NReduce
				reply.TaskType = reducetask
				m.ReduceTaskQ[i].State = doing
				m.ReduceTaskQ[i].StartTime = time.Now()
				break
			}
		}
	case END:
		reply.TaskType = end
	}
	mutex.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if m.Phase == END {
		ret = true
	}
	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.MapTaskNum = len(files)
	m.Phase = MAP_PHASE
	m.NReduce = nReduce
	for i, filename := range files {
		var temp_task MapTask
		temp_task.Title = filename
		temp_task.Id = i
		temp_task.State = notassigned
		m.MapTaskQ = append(m.MapTaskQ, temp_task)
	}
	for i := 0; i < nReduce; i++ {
		var temp_task ReduceTask
		temp_task.Id = i
		temp_task.State = notassigned
		m.ReduceTaskQ = append(m.ReduceTaskQ, temp_task)
	}

	m.server()
	return &m
}
