package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func failOnError(err error) {
	if err != nil {
		lge(err.Error())
		// TODO make sure everything is done before exiting... use global context... for the moment sleep a bit...
		time.Sleep(3 * time.Second)
		os.Exit(0)
	}
}

func setVerboseRunFlag(value bool) {
	printLog = value
}

func lgi(msg string) {
	lg(msg, "INFO")
}

func lge(msg string) {
	lg(msg, "ERROR")
}
func lgw(msg string) {
	lg(msg, "WARNING")
}

func lg(msg string, level string) {
	r := logRecordT{session: sessionId, msg: msg, msgDate: time.Now().Format(time.RFC850), msgUnix: time.Now().UnixMilli(), level: level}
	chLogs <- r
	if printLog && (level != "DEBUG" || printDebug) {
		fmt.Printf("%s %s\n", r.msgDate, r.msg)
	}
}

func ldb(msg string) {
	lg(msg, "DEBUG")
}

func listIntersect(l1 []string, l2 []string) ([]string, bool) {
	var li []string
	var b bool
	for _, v1 := range l1 {
		for _, v2 := range l2 {
			if v1 == v2 {
				li = append(li, v1)
				b = true
			}
		}
	}
	return li, b
}

func splitAndTrim(str string) []string {
	var l, lToReturn []string

	l = strings.Split(str, ",")
	for k := range l {
		l[k] = strings.TrimSpace(l[k])
		if l[k] != "" {
			lToReturn = append(lToReturn, l[k])
		}
	}

	return lToReturn

}
