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
var flagCheckStuck *bool

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
			failOnError(sc.exportHistoryMainLoop())
		}
	}

	if *flagCheckStuck == true {
		lgi(fmt.Sprintf("CHECK TASKS STUCK"))
		//for _, sc := range serversConfigs {
		//	failOnError(sc.checkStuckTasks())
		//}

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
	flagCheckStuck = flag.Bool("checkstucktasks", false, "check running tasks that are stuck")

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
