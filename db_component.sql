create or replace sequence SEQ_TASK_HISTORY_HEADER
    start with 1;

create or replace table MATILLION_TASKS_HISTORY
(
    TASKID         NUMBER,
    PARENTID       NUMBER,
    TYPE           VARCHAR,
    JOBID          NUMBER,
    JOBNAME        VARCHAR,
    JOBREVISION    NUMBER,
    JOBTIMESTAMP   NUMBER,
    COMPONENTID    NUMBER,
    COMPONENTNAME  VARCHAR,
    STATE          VARCHAR,
    ROWCOUNT       NUMBER,
    STARTTIME      NUMBER,
    ENDTIME        NUMBER,
    MESSAGE        VARCHAR,
    TASKBATCHID    NUMBER,
    SPENTTIME      NUMBER,
    TASK_HEADER_ID NUMBER
);

create or replace table MATILLION_TASKS_HISTORY_HEADER
(
    ID                   NUMBER,
    TYPE                 VARCHAR,
    CUTOMERID            NUMBER,
    GROUPNAME            VARCHAR,
    PROJECTID            NUMBER,
    PROJECTNAME          VARCHAR,
    VERSIONID            NUMBER,
    VERSIONNAME          VARCHAR,
    JOBID                NUMBER,
    JOBNAME              VARCHAR,
    ENVIRONMENTID        NUMBER,
    ENVIRONMENTNAME      VARCHAR,
    STATE                VARCHAR,
    ENQUEUEDTIME         NUMBER,
    STARTTIME            NUMBER,
    ENDTIME              NUMBER,
    MESSAGE              VARCHAR,
    ORIGINATORID         VARCHAR,
    ROWCOUNT             NUMBER,
    HASHISTORICJOBS      BOOLEAN,
    JOBNAMES             VARCHAR,
    TASKSCOUNT           NUMBER,
    IMPORTER_INI_SECTION VARCHAR,
    IMPORTER_SID         VARCHAR,
    IMPORTER_IMPORTED    NUMBER,
    SPENTTIME            NUMBER,
    TASK_HEADER_ID       NUMBER,
    IMPORTER_URL         VARCHAR
);

create or replace table MATILLION_TASKS_HISTORY_LOG
(
    MESSAGE      VARCHAR,
    DATE_TEXT    VARCHAR,
    DATE_UNIX    NUMBER,
    IMPORTER_SID VARCHAR,
    LEVEL        VARCHAR
);

