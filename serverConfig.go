package main

import (
	"fmt"
	"time"

	"github.com/flevanti/mth"
)

type serverConfig struct {
	ConfigName         string
	BaseUrl            string
	ApiUser            string
	ApiPassword        string
	GroupsToInclude    []string
	ProjectsToInclude  []string
	IncludeAllGroups   bool
	IncludeAllProjects bool
	Enabled            bool
	client             mth.Client
}

func (s *serverConfig) excludeGroup(group string) (bool, string) {
	if s.IncludeAllGroups {
		return false, "include all groups flag found"
	}
	for _, v := range s.GroupsToInclude {
		if v == group {
			return false, fmt.Sprintf("matched [%s]", group)
		}
	}

	return true, "no match found"
}

func (s *serverConfig) excludeProject(group string, project string) (bool, string) {
	if s.IncludeAllProjects {
		return false, "include all projects flag found"
	}
	for _, v := range s.ProjectsToInclude {
		if v == project {
			return false, fmt.Sprintf("matched [%s]", project)
		}
		if v == fmt.Sprintf("[%s].[%s]", group, project) {
			return false, fmt.Sprintf("matched [%s].[%s]", group, project)
		}
	}

	return true, "no match found "
}

// stm := fmt.Sprintf("select 1 from %s where ID=? and GROUPNAME=? and PROJECTNAME=?", destinationDatabaseConfig.dbTableHeader)

func (s *serverConfig) getLastExportedEndDate(iniSection, group, project string) (time.Time, bool) {
	var lastEndDate time.Time
	var found bool
	db, _ := destinationDatabaseConfig.getDbClient()
	stm := fmt.Sprintf("select max(endtime) as max_endtime from %s where importer_ini_section=? and groupname=? and projectname=? having max_endtime > 0", destinationDatabaseConfig.dbTableHeader)
	res, err := db.Query(stm, iniSection, group, project)
	failOnError(err)
	if res.Next() {
		// found a record, let's retrieve the value
		found = true
		var unix int64
		err = res.Scan(&unix)
		failOnError(err)
		// convert last end date found to unix time and subtract xx days just to be sure
		lastEndDate = time.Unix(unix/1000, 0).AddDate(0, 0, -1)
	} else {
		// no record found, let's assume a good time range to harvest...
		lastEndDate = time.Now().AddDate(0, 0, -50)
	}
	return lastEndDate, found
}

func (s *serverConfig) exportHistoryMainLoop() error {
	var groups, projects []string
	var err error
	var excluded bool
	var excludedWhy string

	lgi("---------------------------------------------------------")
	lgi(fmt.Sprintf("processing INI iniSection [%s]", s.ConfigName))
	if !s.Enabled {
		lgi("iniSection is disabled")
		return nil
	}
	groups, err = s.client.GetGroups()
	if err != nil {
		return err
	}
	for _, group := range groups {
		lgi(fmt.Sprintf("processing group [%s]", group))
		excluded, excludedWhy = s.excludeGroup(group)
		if excluded {
			lgi(fmt.Sprintf("group excluded for the following reason [%s]", excludedWhy))
			continue
		}
		lgi(fmt.Sprintf("group included for the following reason [%s]", excludedWhy))
		projects, err = s.client.GetProjects(group)
		if err != nil {
			return err
		}
		for _, project := range projects {
			lgi(fmt.Sprintf("processing project [%s].[%s]", group, project))
			excluded, excludedWhy = s.excludeProject(group, project)
			if excluded {
				lgi(fmt.Sprintf("project excluded for the following reason [%s]", excludedWhy))
				continue
			}
			lgi(fmt.Sprintf("project included for the following reason [%s]", excludedWhy))

			err = s.exportHistory(s.ConfigName, group, project)
			if err != nil {
				return err
			}

		} // end for loop projects
		lgi("-----------------------------")
	} // end for loop groups

	lgi("---------------------------------------------------------")

	return nil
}

func (s *serverConfig) exportHistory(iniSection, group string, project string) error {
	lgi(fmt.Sprintf("Exporting history for [%s].[%s]", group, project))
	rangeStart, found := s.getLastExportedEndDate(iniSection, group, project)
	rangeEnd := time.Now()
	if found {
		lgi(fmt.Sprintf("Recent exported history found, using it to determine range start"))
	} else {
		lgi(fmt.Sprintf("No previous exported history found, using default range"))
	}
	lgi(fmt.Sprintf("Range is %s --> %s", rangeStart.Format(time.Stamp), rangeEnd.Format(time.Stamp)))

	ch, err := s.client.GetHistoryByRange(group, project, rangeStart, rangeEnd, time.Duration(6*time.Hour), true)
	if err != nil {
		return err
	}
	for taskWrapper := range ch {
		if taskWrapper.Err != nil {
			return taskWrapper.Err
		}
		ldb(fmt.Sprintf("processing task ID %d found in range %s %s -> %s %s", taskWrapper.Task.ID, taskWrapper.TimeRangeStartDate, taskWrapper.TimeRangeStartTime, taskWrapper.TimeRangeEndDate, taskWrapper.TimeRangeEndTime))
		if checkIfTaskAlreadySaved(group, project, taskWrapper.Task.ID) {
			ldb(fmt.Sprintf("Task %d skipped, already processed", taskWrapper.Task.ID))
			if taskWrapper.Task.State == "SUCCESS" {
				fmt.Print("🟦") //already saved - success status
			} else {
				fmt.Print("🟪") //already saved - failed status
			}
			continue
		}
		failOnError(SaveTask(iniSection, sessionId, &taskWrapper.Task))
		if taskWrapper.Task.State == "SUCCESS" {
			fmt.Print("🟩") //saved! - success status
		} else {
			fmt.Print("🟧") //saved - failed status
		}
	}
	fmt.Println()
	return nil
}

func (s *serverConfig) checkRunningTasks() ([]mth.TaskWrapperT, error) {
	//var taskRunning bool
	//var taskQueued bool
	//var taskHasHistory bool
	//var taskAverageQueuedTime int64
	//var taskAverageRunningTime int64
	var tasksRunning []mth.TaskWrapperT

	if !s.Enabled {
		lgi("iniSection is disabled")
		return tasksRunning, nil
	}

	groups, err := s.client.GetGroups()
	if err != nil {
		return tasksRunning, err
	}
	for _, group := range groups {
		projects, err := s.client.GetProjects(group)
		if err != nil {
			return tasksRunning, err
		}
		for _, project := range projects {
			lgi(fmt.Sprintf("Checking matillion project [%s].[%s]", group, project))
			ch, err := s.client.GetRunningTasks(group, project)
			if err != nil {
				return tasksRunning, err
			}
			for taskWrapper := range ch {
				tasksRunning = append(tasksRunning, taskWrapper)
			} //end for ch
		} //end range projects
	} // end range groups
	return tasksRunning, nil
}
