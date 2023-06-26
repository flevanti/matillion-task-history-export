package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/flevanti/goini"
)

var serversConfigs = make(map[string]serverConfig)
var iniSectionsCount int
var printLog = true
var printDebug = false

var sessionId = fmt.Sprintf("%s", time.Now().Format("20060102.150405.Mon"))
var chLogs = make(chan logRecordT, 100)
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc

var flagGetSingleTaskId *bool
var flagIniSection *string
var flagTaskid *int64
var flagVersion *bool
var flagExportHistory *bool
var flagCheckRunningTasks *bool

const INIFILE = "config.ini"
const INCLUDEALL = "*"
const DESTINATIONDATABASESECTION = "SNOWFLAKE"

func main() {
	bootTime := time.Now()

	generateBuildInfo()

	storeFlags()

	checkIfUserWantsToSeeTheVersion()

	// context is not implemented yet, script has been designed to run scheduler, not as a bckground process.
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	defer ctxGlobalCancel()

	lgi(fmt.Sprintf("Process started -- SID [%s]", sessionId))

	// IMPORTING INI CONFIG
	var iniConfig = goini.New()
	failOnError(parseIniFile(iniConfig))
	failOnError(importConfiguration(iniConfig))

	// MATILLION CONFIGURATIONS
	failOnError(createMatillionClients())
	failOnError(checkMatillionCredentials())

	if *flagExportHistory == true {
		lgi(fmt.Sprintf("EXPORTING HISTORY"))
		checkIfUserwantsToRetrieveSpecificTaskIdHistory()

		// DB CONFIGURATION
		failOnError(importDestinationDatabaseConfiguration(iniConfig))
		failOnError(checkDbCredentials())

		// if we are here db credentials are ok, start the db logger
		go dbLogger()

		for _, sc := range serversConfigs {
			lgi(fmt.Sprintf("[%s]", sc.ConfigName))
			failOnError(sc.exportHistoryMainLoop())
		}
	}

	if *flagExportHistory == true && *flagCheckRunningTasks == true {
		fmt.Print("\n\n=====================================================\n\n")
	}

	if *flagCheckRunningTasks == true {
		lgi(fmt.Sprintf("CHECK TASKS RUNNING RIGHT NOW"))
		for _, sc := range serversConfigs {
			lgi(fmt.Sprintf("[%s]", sc.ConfigName))
			tasks, err := sc.checkRunningTasks()
			failOnError(err)
			lgi(fmt.Sprintf("Found %d tasks running...", len(tasks)))
			for _, task := range tasks {
				//unix timestamp includes milliseconds in the integer, remove them, we only want seconds here
				task.Task.StartTime = task.Task.StartTime / 1000
				datetime := time.Unix(task.Task.StartTime, 0)
				lgi(fmt.Sprintf("[%s] %s  %s.%s.%s (v %s) %s %s (%s)",
					task.Task.State, task.Task.EnvironmentName, task.Task.GroupName,
					task.Task.ProjectName, task.Task.JobName, task.Task.VersionName,
					task.Task.Type, datetime.Format("2006-01-02 15:04:05"), time.Since(datetime)))
				//todo decide if we want to do something with the information or check further like average run time for component and so on....
			}
		}
	}

	fmt.Printf("\n\n\nProcess completed, it took %s\n\n", time.Since(bootTime))
	return

}

func storeFlags() {
	flagGetSingleTaskId = flag.Bool("getsingleid", false, "retrieve a single task id history")
	flagIniSection = flag.String("inisection", "", "The name of the ini section to use for the Matillion configuration")
	flagTaskid = flag.Int64("taskid", 0, "The task id to retrieve")
	flagVersion = flag.Bool("version", false, "show version")
	flagExportHistory = flag.Bool("exporthistory", true, "export history")
	flagCheckRunningTasks = flag.Bool("checkrunningtasks", true, "check running tasks")

	flag.Parse()

}
func checkIfUserwantsToRetrieveSpecificTaskIdHistory() {

	var sc serverConfig
	var found bool

	if !*flagGetSingleTaskId {
		return
	}

	lgi("Retrieving specific task id")

	// user is just interested to retrieve a single task id...

	if *flagIniSection == "" {
		lge("[inisection] parameter not found or empty")
		os.Exit(1)
	}
	if *flagTaskid == 0 {
		lge("[taskid] parameter not found or empty")
		os.Exit(1)
	}

	lgi(fmt.Sprintf("Ini section to use is [%s], task id is [%d]", *flagIniSection, *flagTaskid))

	if sc, found = serversConfigs[*flagIniSection]; !found {
		lge(fmt.Sprintf("[inisection] specified [%s] not found", *flagIniSection))
		os.Exit(1)
	}

	result := sc.client.GetHistoryByTaskId(*flagTaskid)

	if result.Err != nil {
		lge(fmt.Sprintf("Error while retrieving task id: %s", result.Err.Error()))
		lge("This probably means that the task ID doesn't exist")
		os.Exit(1)
	}

	jsonString, err := json.Marshal(result.Task)
	if err != nil {
		lge(fmt.Sprintf("error while preparing to output the result: %s", err.Error()))
		os.Exit(1)
	}

	fmt.Printf("%s\n", jsonString)

	os.Exit(0)

}
