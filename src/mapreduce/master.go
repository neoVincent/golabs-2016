package mapreduce

import (
	"container/list"
	"errors"
	"log"
	"sync"
)
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	lastJobId int
	jobType   JobType
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

func (mr *MapReduce) scheduleJob(worker WorkerInfo, args DoJobArgs, waitGroup *sync.WaitGroup) {

	// update info in worker
	worker.lastJobId = args.JobNumber
	worker.jobType = args.Operation

	var reply DoJobReply

	ok := call(worker.address, "Worker.DoJob", args, &reply)
	if ok == false {
		log.Printf("RunMaster: worker %s  jobnum %v map failed \t reply: %t\n", worker.address, args.JobNumber, reply.OK)
		mr.failedWorkers <- worker
		waitGroup.Done()
	} else {
		log.Printf("RunMaster: worker %s  jobnum %v map success!\n", worker.address, args.JobNumber)
		delete(mr.jobStats, worker.lastJobId) // update the last job state
		mr.availableWorkers <- worker
		waitGroup.Done()
	}
}

func (mr *MapReduce) redoJob(worker WorkerInfo, waitGroup *sync.WaitGroup) {

	jobArgs := DoJobArgs{
		File:          mr.file,
		Operation:     worker.jobType,
		JobNumber:     worker.lastJobId,
		NumOtherPhase: mr.nReduce,
	}

	log.Printf("RunMaster: %s redo the job %s \t %v \n", worker.address, worker.jobType, jobArgs.JobNumber)

	if worker.jobType == Reduce {
		jobArgs.NumOtherPhase = mr.nMap
	}

	mr.scheduleJob(worker, jobArgs, waitGroup)
}

func (mr *MapReduce) getNextJob(jobType JobType) (DoJobArgs, error) {

	jobArgs := DoJobArgs{
		File:          mr.file,
		Operation:     Map,
		JobNumber:     0,
		NumOtherPhase: mr.nReduce,
	}
	switch jobType {
	case Map:
		jobArgs.Operation = Map
		jobArgs.NumOtherPhase = mr.nReduce
		if len(mr.jobStats) != 0 && len(mr.mapJobs) == 0 {
			return jobArgs, errors.New("Pending")
		}

		if len(mr.mapJobs) == 0 {
			return jobArgs, errors.New("JobDone")
		}

		jobNum := mr.mapJobs[0]
		if len(mr.mapJobs) > 1 {
			mr.mapJobs = mr.mapJobs[1:]
		} else {
			mr.mapJobs = []int{}
		}
		mr.jobStats[jobNum] = true
		jobArgs.JobNumber = jobNum
		return jobArgs, nil

	case Reduce:
		jobArgs.Operation = Reduce
		jobArgs.NumOtherPhase = mr.nMap
		if len(mr.jobStats) != 0 && len(mr.reduceJobs) == 0 {
			return jobArgs, errors.New("Pending")
		}

		if len(mr.reduceJobs) == 0 {
			return jobArgs, errors.New("JobDone")
		}

		jobNum := mr.reduceJobs[0]
		if len(mr.reduceJobs) > 1 {
			mr.reduceJobs = mr.reduceJobs[1:]
		} else {
			mr.reduceJobs = []int{}
		}
		mr.jobStats[jobNum] = true
		jobArgs.JobNumber = jobNum
		return jobArgs, nil
	}
	return jobArgs, nil
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Workers = make(map[string]*WorkerInfo)

	waitGroup := new(sync.WaitGroup)
	jobsDone := false
	var worker WorkerInfo
	for !jobsDone {
		select {
		case regWorker := <-mr.registerChannel:
			workerInfo := &WorkerInfo{address: regWorker, lastJobId: -1, jobType: Map}
			mr.Workers[regWorker] = workerInfo
			go func() { mr.availableWorkers <- *mr.Workers[regWorker] }()
		case worker = <-mr.failedWorkers:
			waitGroup.Add(1)
			go mr.redoJob(worker, waitGroup)
		case worker = <-mr.availableWorkers:
			jobArgs, err := mr.getNextJob(worker.jobType)
			if err != nil {
				if err.Error() == "JobDone" {
					if len(mr.mapJobs) == 0 && len(mr.reduceJobs) == 0 {
						jobsDone = true
						break
					} else if len(mr.mapJobs) == 0 {
						// start Reducing
						log.Printf("========RunMaster: REDUCE START===========")
						jobArgs, err = mr.getNextJob(Reduce)
					}
				} else {
					// pending
					// reuse this worker
					log.Printf("RunMaster: Pending %v", len(mr.jobStats))
					go func() { mr.availableWorkers <- worker }()
					break
				}
			}
			waitGroup.Add(1)
			go mr.scheduleJob(worker, jobArgs, waitGroup)
		}
	}
	waitGroup.Wait()
	return mr.KillWorkers()

	//mapJobsDone = false
	//for !mapJobsDone {
	//	//var worker string
	//	select {
	//	case worker = <-mr.registerChannel:
	//		workerInfo := &WorkerInfo{address: worker}
	//		mr.Workers[worker] = workerInfo
	//	case worker = <-mr.failedMapWorker:
	//	case worker = <-mr.availableWorkers:
	//	}
	//
	//	jobId, err := mr.getNextReduceJob()
	//	if err != nil {
	//		if err.Error() == "JobDone" {
	//			mapJobsDone = true
	//			go mr.appendWorker(worker)
	//			break
	//		}
	//		go mr.appendWorker(worker)
	//		continue
	//	}
	//	log.Printf("RunMster: getNextReduceJob id  %v\n", jobId)
	//	rargs.JobNumber = jobId
	//	waitGroup.Add(1)
	//	go mr.scheduleReduceJob(worker, rargs, waitGroup)
	//}
	//waitGroup.Wait()
	//log.Printf("RunMaster: Reduce FINISHED!!!")

}
