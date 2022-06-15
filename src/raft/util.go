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
	found := false
	for l < r {
		mid := (l + r) / 2
		if entries[mid].Term < target {
			l = mid + 1
		} else {
			if entries[mid].Term == target {
				found = true
			}
			r = mid
		}
	}
	return l, found
}

func statusToStr(s PeerStatus) string {
	str := ""
	switch s {
	case Leader:
		str = "Le"
	case Candidate:
		str = "Ca"
	case Follower:
		str = "Fo"
	}
	return str
}
