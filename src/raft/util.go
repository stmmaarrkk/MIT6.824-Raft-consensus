package raft

import (
	"log"
	"math"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Max(n1 int, n2 int) int {
	return int(math.Max(float64(n1), float64(n2)))
}
func Min(n1 int, n2 int) int {
	return int(math.Min(float64(n1), float64(n2)))
}

func binarySearch(entries []Entry, target int) (int, bool) {
	l := 0
	r := len(entries)
	for l < r {
		mid := (l + r) / 2
		if entries[mid].Term == target {
			r = mid + 1
		} else if entries[mid].Term < target {
			l = mid + 1
		} else {
			r = mid
		}
	}

	if l < len(entries) || len(entries) == 0 {
		return l, false
	} else {
		return l, (entries[l].Term == target)
	}
}
