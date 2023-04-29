package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/flevanti/mth"
	sf "github.com/snowflakedb/gosnowflake"
)

type destinationDatabaseConfigT struct {
	account        string
	user           string
	password       string
	dbUserRole     string
	dbName         string
	dbSchema       string
	dbWarehouse    string
	dbTableHeader  string
	dbTableDetails string
	dbTableLog     string
	dbPort         int
	useLog         bool
	dbClient       *sql.DB
}

func (c *destinationDatabaseConfigT) getDsn() (string, error) {
	cfg := &sf.Config{
		Account:   c.account,
		User:      c.user,
		Password:  c.password,
		Role:      c.dbUserRole,
		Warehouse: c.dbWarehouse,
		Schema:    c.dbSchema,
		Database:  c.dbName,
		Port:      c.dbPort,
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		return "", err
	}
	return dsn, nil
}

func (c *destinationDatabaseConfigT) getDbClient() (*sql.DB, error) {
	if c.dbClient != nil {
		return c.dbClient, nil
	}
	dsn, err := c.getDsn()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}
	c.dbClient = db
	return db, nil
}

type logRecordT struct {
	session string
	msg     string
	msgDate string
	msgUnix int64
	level   string
}

var destinationDatabaseConfig destinationDatabaseConfigT

func checkDbCredentials() error {
	var stm string

	lgi("Checking database credentials")
	db, err := destinationDatabaseConfig.getDbClient()
	if err != nil {
		return err
	}

	lgi("Database credentials ok")

	// use the same contextWithTimeout for the whole operation....
	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Duration(5*time.Second))
	defer ctxCancel()

	// HEADER TABLE
	stm = fmt.Sprintf("select 1 from \"%s\".\"%s\".\"%s\" limit 1;", destinationDatabaseConfig.dbName, destinationDatabaseConfig.dbSchema, destinationDatabaseConfig.dbTableHeader)
	_, err = db.QueryContext(ctx, stm)
	if err != nil {
		return err
	}
	lgi(fmt.Sprintf("Permission on main table ok [%s.%s.%s]", destinationDatabaseConfig.dbName, destinationDatabaseConfig.dbSchema, destinationDatabaseConfig.dbTableHeader))

	// DETAIL TABLE
	stm = fmt.Sprintf("select 1 from \"%s\".\"%s\".\"%s\" limit 1;", destinationDatabaseConfig.dbName, destinationDatabaseConfig.dbSchema, destinationDatabaseConfig.dbTableDetails)
	_, err = db.QueryContext(ctx, stm)
	if err != nil {
		return err
	}
	lgi(fmt.Sprintf("Permission on main table ok [%s.%s.%s]", destinationDatabaseConfig.dbName, destinationDatabaseConfig.dbSchema, destinationDatabaseConfig.dbTableDetails))

	// LOG TABLE
	if destinationDatabaseConfig.useLog {
		stm = fmt.Sprintf("select 1 from \"%s\".\"%s\".\"%s\" limit 1;", destinationDatabaseConfig.dbName, destinationDatabaseConfig.dbSchema, destinationDatabaseConfig.dbTableLog)
		_, err = db.QueryContext(ctx, stm)
		if err != nil {
			return err
		}
		lgi(fmt.Sprintf("Permission on log table ok [%s.%s.%s]", destinationDatabaseConfig.dbName, destinationDatabaseConfig.dbSchema, destinationDatabaseConfig.dbTableLog))
	} else {
		lgi("Logging to db disabled, permissions on log table skipped")
	}

	return nil
}

func checkIfTaskAlreadySaved(groupName, projectName string, taskId int) bool {
	stm := fmt.Sprintf("select 1 from %s where ID=? and GROUPNAME=? and PROJECTNAME=?", destinationDatabaseConfig.dbTableHeader)

	db, _ := destinationDatabaseConfig.getDbClient()

	res, err := db.Query(stm, taskId, groupName, projectName)
	failOnError(err)

	return res.Next()

}

func SaveTask(inisection, sessionId string, t *mth.TaskT) error {
	var values []interface{}

	db, _ := destinationDatabaseConfig.getDbClient()

	taskHeaderId, err := generateTaskHeaderSequenceId()
	if err != nil {
		return err
	}

	values = append(values, t.ID, t.Type, t.CustomerID, t.GroupName, t.ProjectID, t.ProjectName, t.VersionID, t.VersionName, t.JobID,
		t.JobName, t.EnvironmentID, t.EnvironmentName, t.State, t.EnqueuedTime, t.StartTime, t.EndTime, t.EndTime-t.StartTime, t.Message,
		t.OriginatorID, t.RowCount, t.HasHistoricJobs, strings.Join(t.JobNames, "||"), len(t.Tasks), inisection, sessionId, time.Now().Unix(), taskHeaderId, serversConfigs[inisection].BaseUrl)

	stm := fmt.Sprintf("insert into %s (ID, TYPE, CUTOMERID, GROUPNAME, PROJECTID, PROJECTNAME, VERSIONID, VERSIONNAME, JOBID, "+
		"JOBNAME, ENVIRONMENTID, ENVIRONMENTNAME, STATE, ENQUEUEDTIME, STARTTIME, ENDTIME, SPENTTIME, MESSAGE, "+
		"ORIGINATORID, ROWCOUNT, HASHISTORICJOBS, JOBNAMES, TASKSCOUNT, IMPORTER_INI_SECTION, IMPORTER_SID, IMPORTER_IMPORTED,TASK_HEADER_ID, IMPORTER_URL) values (%s)", destinationDatabaseConfig.dbTableHeader, strings.TrimRight(strings.Repeat("?,", len(values)), ","))

	// start a transaction
	// please don't write db logs during a transaction, they could end up lost if transaction is rolled back.
	txOpts := sql.TxOptions{}
	tx, err := db.BeginTx(context.Background(), &txOpts)

	if err != nil {
		return err
	}
	_, err = db.Exec(stm, values...)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = SaveTaskChildren(taskHeaderId, t)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()

	return err
}

func SaveTaskChildren(taskHeaderId int, taskRoot *mth.TaskT) error {

	if len(taskRoot.Tasks) == 0 {
		return nil
	}

	var values []interface{}
	var valuesPlaceholders string
	var statementCreated bool
	var stm string
	var sequence int64

	for _, t := range taskRoot.Tasks {
		sequence++

		values = append(values, t.TaskID, t.ParentID, t.Type, t.JobID, t.JobName, t.JobRevision, t.JobTimestamp, t.ComponentID, t.ComponentName, t.State,
			t.RowCount, t.StartTime, t.EndTime, t.EndTime-t.StartTime, t.Message, t.TaskBatchID, taskHeaderId, sequence)

		// this is inside the loop after the values of the first record has been added so we know how many values for each record and we can generate the placeholders
		// accordingly.
		// just to avoid to have to count them and hardcode a number
		if !statementCreated {
			statementCreated = true
			valuesPlaceholders = "(" + strings.TrimRight(strings.Repeat("?,", len(values)), ",") + "),"
			valuesPlaceholders = strings.Repeat(valuesPlaceholders, len(taskRoot.Tasks))
			valuesPlaceholders = strings.TrimRight(valuesPlaceholders, ",")
			stm = "insert into MATILLION_TASKS_HISTORY (TASKID, PARENTID, TYPE, JOBID, JOBNAME, JOBREVISION, JOBTIMESTAMP, COMPONENTID, COMPONENTNAME, STATE, " +
				"ROWCOUNT, STARTTIME, ENDTIME, SPENTTIME, MESSAGE, TASKBATCHID,TASK_HEADER_ID, TASK_SEQUENCE) VALUES " + valuesPlaceholders
		}
	}

	db, _ := destinationDatabaseConfig.getDbClient()
	_, err := db.Exec(stm, values...)
	if err != nil {
		return err
	}

	return nil

}

func generateTaskHeaderSequenceId() (int, error) {
	db, _ := destinationDatabaseConfig.getDbClient()

	stm := "select SEQ_TASK_HISTORY_HEADER.nextval as id"
	result, err := db.Query(stm)
	if result.Err() != nil {
		return 0, result.Err()
	}
	b := result.Next()
	if b == false {
		return 0, errors.New("unable to create sequence ID for header table")
	}
	var id int
	err = result.Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func dbLogger() {
	var logRec logRecordT
	db, err := destinationDatabaseConfig.getDbClient()
	stm := fmt.Sprintf("insert into %s (IMPORTER_SID, MESSAGE, DATE_TEXT, DATE_UNIX, LEVEL) VALUES (?,?,?,?,?)", destinationDatabaseConfig.dbTableLog)
	failOnError(err)
	for {
		select {
		case logRec = <-chLogs:
			if destinationDatabaseConfig.useLog {
				_, err = db.Exec(stm, logRec.session, logRec.msg, logRec.msgDate, logRec.msgUnix, logRec.level)
				failOnError(err)
			}
		case <-ctxGlobal.Done():
			// todo make sure all logs are saved before returning...
			return
		}
	}
}
