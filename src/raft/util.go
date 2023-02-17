package raft

import "log"

// Debugging
//const Debug = true
//const Debuga = false
//const Debugb = true

const Debug = false
const Debuga = false
const Debugb = false

func Lab2Printf(format string, a ...interface{}) (n int) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func Lab2aPrintf(format string, a ...interface{}) (n int) {
	if Debuga {
		log.Printf(format, a...)
	}
	return
}
func Lab2bPrintf(format string, a ...interface{}) (n int) {
	if Debugb {
		log.Printf(format, a...)
	}
	return
}
