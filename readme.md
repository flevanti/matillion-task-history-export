# Matillion Task History Export

This tool can be used to read one or more matillion servers history using the internal API and save the information
harvested in a couple of database tables.  
The Database used to store the history is snowflake.

This tool has been built on top of  `MTK - Matillion Task History` package available at https://github.com/flevanti/mth

## Configuration

Rename the file `example.config.ini` to `config.ini`.

### Matillion servers

You can add as many server Matillion servers as you want, the application will loop through them one by one. Just add an
additional `[INI SECTION]` in the ini file and add the relevant keys.  
These are the keys required to configure a Matillion server:

| KEY               | VALUE                                                                                                                                                                                    |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| BASEURL           | The url you usually use to connect to the matillion instance                                                                                                                             |
| APIUSER           | The user to use to query the Matillion API                                                                                                                                               |
| APIPASSWORD       | The password assiciated with the user                                                                                                                                                    |
| GROUPSTOINCLUDE   | The groups to include in the export. `*` can be used to export all groups                                                                                                                |
| PROJECTSTOINCLUDE | The project to include in the export. `*` can be used to export all project. if you want to specify only a specific project under a specific group use di the syntax `[group].[project]` |
| ENABLED           | if value is `1` the application will use the configuration otherwise it will be skipped.                                                                                                 |

_Please note: user needs to be configured in Matillion with API privileges. It is reccomended to create a dedicated user
for this task._

### Snowflake server

The snowflake server configuration in the ini file must be named `[SNOWFLAKE]`.

| KEY            | VALUE                                                                                                                                                             |
|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ACCOUNT        | The snowflake account used to connect. This is usually the value in the url `http://[account].snowflakecomputing.com`                                             |
| DBUSER         | The database username to use for the connection                                                                                                                   |
| DBPASSWORD     | The password associated with the username                                                                                                                         |
| DBUSERROLE     | The role associated to the username to use for the operation                                                                                                      |
| DBWAREHOUSE    | The warehouse to use                                                                                                                                              |
| DBNAME         | The database used to store data                                                                                                                                   |
| DBSCHEMA       | The schema used to store data                                                                                                                                     |
| DBTABLEHEADER  | The table name used to store matillion tasks history headers. Something like `MATILLION_TASKS_HISTORY_HEADER`                                                     |
| DBTABLEDETAILS | The table name used to store matillion tasks history details. Something like `MATILLION_TASKS_HISTORY`                                                            |
| DBTABLELOG     | The table name used to store operation logs. Something like `MATILLION_TASKS_HISTORY_LOG`. This table is not mandatory, leave the value empty to disable logging. |

_Please note: User needs to have access to the database/schema configured and the tables need to exist.  
The app won't create the tables and it will just crash if they are not there or accessible.  
The tables ddl can be found in the file `db_components.sql`. Make sure the tables (and the sequence) exist and the user
can access them._
