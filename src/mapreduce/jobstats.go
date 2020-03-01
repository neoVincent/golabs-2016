package mapreduce

import "sync"

type JobStats struct {
	sync.RWMutex
	stats map[int]bool
}

func newJobStats() *JobStats {
	js := new(JobStats)
	js.stats = make(map[int]bool)
	return js

}

func (js *JobStats) readMap(key int) bool {
	js.RLock()
	val := js.stats[key]
	js.RUnlock()
	return val
}

func (js *JobStats) writeMap(key int, val bool) {
	js.Lock()
	js.stats[key] = val
	js.Unlock()
}

func (js *JobStats) deleteMap(key int) {
	js.Lock()
	delete(js.stats, key)
	js.Unlock()
}

func (js *JobStats) length() int {
	js.RLock()
	len := len(js.stats)
	js.RUnlock()
	return len
}
