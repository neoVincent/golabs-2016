package mapreduce

import (
	"container/list"
	"errors"
	"log"
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

func (mr *MapReduce) scheduleJob(worker WorkerInfo, args DoJobArgs) {

	// update info in worker
	worker.lastJobId = args.JobNumber
	worker.jobType = args.Operation

	var reply DoJobReply

	ok := call(worker.address, "Worker.DoJob", args, &reply)
	if ok == false {
		log.Printf("RunMaster: worker %s  jobnum %v %s failed \t reply: %t\n", worker.address, args.JobNumber, worker.jobType, reply.OK)
		mr.failedWorkers <- worker
		mr.jobStats.deleteMap(worker.lastJobId)
	} else {
		log.Printf("RunMaster: worker %s  jobnum %v %s success!\n", worker.address, args.JobNumber, worker.jobType)
		mr.jobStats.deleteMap(worker.lastJobId)
		mr.availableWorkers <- worker
	}
}

func (mr *MapReduce) redoJob(worker WorkerInfo) {
	// add back to job queue
	switch worker.jobType {
	case Map:
		mr.mapJobs = append(mr.mapJobs, worker.lastJobId)
	case Reduce:
		mr.reduceJobs = append(mr.reduceJobs, worker.lastJobId)
	}
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
		if mr.jobStats.length() != 0 && len(mr.mapJobs) == 0 {
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
		mr.jobStats.writeMap(jobNum, true)

		jobArgs.JobNumber = jobNum
		return jobArgs, nil

	case Reduce:
		jobArgs.Operation = Reduce
		jobArgs.NumOtherPhase = mr.nMap
		if mr.jobStats.length() != 0 && len(mr.reduceJobs) == 0 {
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
		mr.jobStats.writeMap(jobNum, true)

		jobArgs.JobNumber = jobNum
		return jobArgs, nil
	}
	return jobArgs, nil
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Workers = make(map[string]*WorkerInfo)

	jobsDone := false
	var worker WorkerInfo
	for !jobsDone {
		select {
		case regWorker := <-mr.registerChannel:
			workerInfo := &WorkerInfo{address: regWorker, lastJobId: -1, jobType: Map}
			mr.Workers[regWorker] = workerInfo
			go func() { mr.availableWorkers <- *mr.Workers[regWorker] }()
		case worker = <-mr.failedWorkers:
			go mr.redoJob(worker)
		case worker = <-mr.availableWorkers:
			jobArgs, err := mr.getNextJob(worker.jobType)
			if err != nil {
				if err.Error() == "JobDone" {
					if len(mr.mapJobs) == 0 && len(mr.reduceJobs) == 0 {
						log.Printf("========RunMaster: JOB DONE===========")
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
					//log.Printf("RunMaster: Pending %v", mr.jobStats.length())
					//log.Printf("RunMaster: Worker %s\t%v\t%s", worker.address,worker.lastJobId,worker.jobType)
					go func() { mr.availableWorkers <- worker }()
					continue
				}
			}
			go mr.scheduleJob(worker, jobArgs)
		}
	}
	return mr.KillWorkers()

}
