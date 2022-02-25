package main

import (
	"context"
	"fmt"
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

const INIFILE = "config.ini"
const INCLUDEALL = "*"
const DESTINATIONDATABASESECTION = "SNOWFLAKE"

func main() {
	bootTime := time.Now()
	// context is not implemented yet, script has been designed to run scheduler, not as a bckground process.
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	defer ctxGlobalCancel()

	lgi(fmt.Sprintf("Process started -- SID [%s]", sessionId))
	var iniConfig = goini.New()
	failOnError(parseIniFile(iniConfig))
	failOnError(importConfiguration(iniConfig))
	failOnError(importDestinationDatabaseConfiguration(iniConfig))
	failOnError(createMatillionClients())
	failOnError(checkMatillionCredentials())
	failOnError(checkDbCredentials())

	// if we are here db credentials are ok, start the db logger
	go dbLogger()

	lgi("Exporting task history")
	for _, sc := range serversConfigs {
		failOnError(sc.exportHistoryMainLoop())
	}

	fmt.Printf("\n\n\nProcess completed, it took %s\n\n", time.Since(bootTime))
	return

}
