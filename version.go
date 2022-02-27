package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"
)

var buildDateUnix string = "0"
var buildDate string
var build string
var version string = "developer"

func generateBuildInfo() {
	// TODO ADD BUILD INFO RELATED TO GIT IF BUILD IS GOING TO HAPPEN AUTOMATICALLY
	var buildDateUnix64, err = strconv.ParseInt(buildDateUnix, 10, 64)
	failOnError(err)
	buildDate = time.Unix(buildDateUnix64, 0).Format(time.RFC850)
	hasher := md5.New()
	hasher.Write([]byte(strconv.FormatInt(buildDateUnix64, 10)))
	build = hex.EncodeToString(hasher.Sum(nil))[:7]
}

func checkIfUserWantsToSeeTheVersion() {
	if *flagVersion {
		printVersion()
		os.Exit(1)
	}
}

func printVersion() {
	fmt.Printf("VERSION...... %s\n", version)
	fmt.Printf("BUILD DATE... %s\n", buildDate)
	fmt.Printf("BUILD UNIX... %s\n", buildDateUnix)
	fmt.Printf("BUILD HASH... %s\n", build)

}
