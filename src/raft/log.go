package raft

type Entry string

type Log struct {
	len            int
	entries        []Entry
	snapshotOffset int
}

func makeLog(entries []Entry) Log {
	log := Log{}
	log.len = 0
	log.entries = entries
	log.snapshotOffset = 0
	return log
}

//access log at certain index
func (log *Log) at(pos int) Entry {
	if pos == -1 {
		pos = len(log.entries) - 1
	}
	return log.entries[pos]
}

func (log *Log) getEntries(start int, end int) []Entry {
	//end is exclusive
	entries := make([]Entry, 0)
	if start < end {
		i := 0
		i++
		//it need to be complete
	}
	return entries
}

func (log *Log) append(entries []Entry) bool {
	//TODO
	return true
}

//not thread-save
func (log *Log) size() int {
	return len(log.entries)
}
