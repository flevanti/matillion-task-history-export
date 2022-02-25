package main

import (
	"errors"
	"fmt"

	"github.com/flevanti/goini"
	"github.com/flevanti/mth"
)

func createMatillionClients() error {
	for k, v := range serversConfigs {
		lgi(fmt.Sprintf("Creating matillion client for section [%s]", k))
		v.client = mth.New(v.BaseUrl, v.ApiUser, v.ApiPassword)
		serversConfigs[k] = v
	}
	return nil
}

func checkMatillionCredentials() error {
	for k, v := range serversConfigs {
		lgi(fmt.Sprintf("Checking matillion client for section [%s]", k))
		err := v.client.CheckConnection()
		if err != nil {
			return err
		}
	}
	return nil
}

func parseIniFile(iniConfig *goini.INI) error {
	lgi("Parsing INI file")
	failOnError(iniConfig.ParseFile(INIFILE))
	iniSectionsCount = len(iniConfig.GetSectionsList())
	lgi(fmt.Sprintf("%d sections found", iniSectionsCount))

	if iniSectionsCount > 0 {
		lgi(fmt.Sprintf("%v", iniConfig.GetSectionsList()))
		return nil
	}
	return errors.New("no sections found in the ini file")
}

func importConfiguration(iniConfig *goini.INI) error {
	for _, section := range iniConfig.GetSectionsList() {
		if section == DESTINATIONDATABASESECTION {
			lgi(fmt.Sprintf("Section [%s] configuration will be imported later", section))
			continue
		}
		lgi(fmt.Sprintf("Importing configuration for section [%s]", section))
		err := importConfigurationSection(section, iniConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

func importDestinationDatabaseConfiguration(iniConfig *goini.INI) error {
	lgi(fmt.Sprintf("Importing section [%s]", DESTINATIONDATABASESECTION))

	var found bool
	if destinationDatabaseConfig.account, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "ACCOUNT"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBHOST] in [%s] section", DESTINATIONDATABASESECTION))
	}
	if destinationDatabaseConfig.user, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "DBUSER"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBUSER] in [%s] section", DESTINATIONDATABASESECTION))
	}
	if destinationDatabaseConfig.password, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "DBPASSWORD"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBPASSWORD] in [%s] section", DESTINATIONDATABASESECTION))
	}
	if destinationDatabaseConfig.dbUserRole, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "DBUSERROLE"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBUSERROLE] in [%s] section", DESTINATIONDATABASESECTION))
	}
	if destinationDatabaseConfig.dbName, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "DBNAME"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBNAME] in [%s] section", DESTINATIONDATABASESECTION))
	}
	if destinationDatabaseConfig.dbSchema, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "DBSCHEMA"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBSCHEMA] in [%s] section", DESTINATIONDATABASESECTION))
	}
	if destinationDatabaseConfig.dbTableHeader, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "DBTABLEHEADER"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBTABLEHEADER] in [%s] section", DESTINATIONDATABASESECTION))
	}
	if destinationDatabaseConfig.dbTableDetails, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "DBTABLEDETAILS"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBTABLEDETAILS] in [%s] section", DESTINATIONDATABASESECTION))
	}
	if destinationDatabaseConfig.dbTableLog, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "DBTABLELOG"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBTABLELOG] in [%s] section", DESTINATIONDATABASESECTION))
	}
	if destinationDatabaseConfig.dbWarehouse, found = iniConfig.SectionGet(DESTINATIONDATABASESECTION, "DBWAREHOUSE"); !found {
		return errors.New(fmt.Sprintf("unable to find [DBWAREHOUSE] in [%s] section", DESTINATIONDATABASESECTION))
	}

	if destinationDatabaseConfig.dbTableLog != "" {
		destinationDatabaseConfig.useLog = true
	}
	return nil
}

func importConfigurationSection(section string, ini *goini.INI) error {
	var found bool
	var stringToSplit string
	if _, b := serversConfigs[section]; b {
		return errors.New(fmt.Sprintf("section [%s] already present in configuration", section))
	}
	c := serverConfig{}
	c.ConfigName = section

	c.BaseUrl, found = ini.SectionGet(section, "BASEURL")
	if !found {
		return errors.New("[BASEURL] not found in section [section]")
	}

	c.ApiUser, found = ini.SectionGet(section, "APIUSER")
	if !found {
		return errors.New("[APIUSER] not found in section [section]")
	}

	c.ApiPassword, found = ini.SectionGet(section, "APIPASSWORD")
	if !found {
		return errors.New("[APIPASSWORD] not found in section [section]")
	}

	c.Enabled, found = ini.SectionGetBool(section, "ENABLED")
	if !found {
		return errors.New("[ENABLED] not found in section [section]")
	}

	stringToSplit, found = ini.SectionGet(section, "GROUPSTOINCLUDE")
	if !found || stringToSplit == "" {
		return errors.New("[GROUPSTOINCLUDE] not found or empty in section [section]")
	}
	if stringToSplit == INCLUDEALL {
		c.IncludeAllGroups = true
	}
	c.GroupsToInclude = splitAndTrim(stringToSplit)

	stringToSplit, found = ini.SectionGet(section, "PROJECTSTOINCLUDE")
	if !found || stringToSplit == "" {
		return errors.New("[PROJECTSTOINCLUDE] not found or empty in section [section]")
	}
	if stringToSplit == INCLUDEALL {
		c.IncludeAllProjects = true
	}
	c.ProjectsToInclude = splitAndTrim(stringToSplit)

	serversConfigs[section] = c

	return nil
}
