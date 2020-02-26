package mapreduce

import (
	"container/list"
	"log"
	"sync"
)
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func scheduleJob(worker string, args *DoJobArgs, waitGroup *sync.WaitGroup, workerChan chan string) {
	defer func() {
		waitGroup.Done()
		workerChan <- worker
	}()
	var reply DoJobReply
	ok := call(worker, "Worker.DoJob", args, &reply)
	if ok == false {
		log.Printf("RunMaster: worker %s  jobnum %v map failed \t reply: %t\n", worker, args.JobNumber, reply.OK)
		// handle timeout or server unreachable
	} else {
		log.Printf("RunMaster: worker %s  jobnum %v map success!\n", worker, args.JobNumber)
	}
}
func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Workers = make(map[string]*WorkerInfo)

	availableWorkers := make(chan string)
	waitGroup := new(sync.WaitGroup)

	log.Printf("RunMaster: map jobs: %v \treduced jobs: %v\n", mr.nMap, mr.nReduce)
	for i := 0; i < mr.nMap; i++ {
		log.Printf("RunMaster: nMap id: %v", i)
		var worker string
		select {
		case worker = <-availableWorkers:
			log.Printf("RunMaster: available worker %v \n", worker)
		case worker = <-mr.registerChannel:
			log.Printf("RunMaster: usable worker: %v \n", worker)
			//default:
			//	log.Printf("RunMaster: No usable worker wait for next loop ...\n")
			//	i--
			//	continue
		}
		log.Printf("RunMster: Current worker %s\n", worker)

		waitGroup.Add(1)
		mr.Workers[worker] = &WorkerInfo{worker}
		margs := &DoJobArgs{
			File:          mr.file,
			Operation:     "Map",
			JobNumber:     i,
			NumOtherPhase: mr.nReduce,
		}

		go scheduleJob(worker, margs, waitGroup, availableWorkers)
	}
	waitGroup.Wait()

	log.Printf("RunMaster: Map FINISHED!!!")
	log.Printf("RunMaster: number of workers %v", len(mr.Workers))

	for i := 0; i < mr.nReduce; i++ {
		var worker string
		select {
		case worker = <-availableWorkers:
			log.Printf("RunMaster: available worker %v \n", worker)
		case worker = <-mr.registerChannel:
			log.Printf("RunMaster: usable worker: %v \n", worker)
			//default:
			//	log.Printf("RunMaster: No usable worker wait for next loop ...\n")
			//	i--
			//	continue
		}
		waitGroup.Add(1)
		rargs := &DoJobArgs{
			File:          mr.file,
			Operation:     "Reduce",
			JobNumber:     i,
			NumOtherPhase: mr.nMap,
		}
		go scheduleJob(worker, rargs, waitGroup, availableWorkers)
	}
	waitGroup.Wait()
	log.Printf("RunMaster: Reduce FINISHED!!!")
	return mr.KillWorkers()

}
