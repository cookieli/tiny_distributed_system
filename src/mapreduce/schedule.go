package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	/*when function start several gorouting if it want after these finish then it finish ,you can use waitgroup
	*/
	var task DoTaskArgs
	task.JobName = jobName
	task.Phase = phase
	task.NumOtherPhase = n_other
	taskChan := make(chan int)
	wg :=sync.WaitGroup{}
	go func() {
		for i := 0; i < ntasks; i++ {
			wg.Add(1)
			taskChan <- i
		}
		wg.Wait()
		close(taskChan)
	}()
	for i:= range taskChan {
		worker := <- registerChan
		if phase == mapPhase{
			task.File = mapFiles[i]
		}
		task.TaskNumber = i
		go func(worker string, task DoTaskArgs) {
			if call(worker, "Worker.DoTask", task, nil) {
				wg.Done()
				//log.Printf("the worker %s works down and we can add it to register\n", worker)
				registerChan<- worker
			} else {

				log.Printf("the worker fails add we need to rearrange the task\n")
				taskChan<- task.TaskNumber
			}
		}(worker, task)

	}

	/*for i:= 0; i < ntasks; i++ {
		worker := <- registerChan
		if phase == mapPhase{
			task.File = mapFiles[i]
		}
		task.TaskNumber = i
		if call(worker, "Worker.DoTask", task, nil) {
			go func() {registerChan <-worker}()
		} else {
			//now we need to reassin tasks
		}
	}*/

	fmt.Printf("Schedule: %v phase done\n", phase)
	//wg.Wait()

}
