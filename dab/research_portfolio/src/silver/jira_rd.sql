-- Silver layer for Jira R&D (project tasks). Streaming tables read from the bronze Delta tables.
-- Bronze uses camelCase, nested structs and epoch milliseconds; silver flattens the structs and converts to timestamps.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${jira_rd_silver_schema}`.`02_issue` (
  issueKey STRING COMMENT 'Jira issue key, for example RD0001-12.',
  projectId STRING COMMENT 'Canonical project identifier (PRJ-0001 format) derived from the Jira project key.',
  summary STRING COMMENT 'Short description of the task.',
  issueType STRING COMMENT 'Issue type: Task, Bug, Story or Sub-task.',
  statusName STRING COMMENT 'Current workflow status of the issue.',
  isResolved BOOLEAN COMMENT 'True when the issue status category is done.',
  priorityName STRING COMMENT 'Priority of the issue (Highest, High, Medium or Low).',
  assigneeName STRING COMMENT 'Display name of the assignee.',
  assigneeEmail STRING COMMENT 'Email address of the assignee.',
  assigneeAccountId STRING COMMENT 'Jira account identifier of the assignee.',
  storyPoints DOUBLE COMMENT 'Estimated effort in story points.',
  createdAt TIMESTAMP COMMENT 'Timestamp when the issue was created.',
  resolvedAt TIMESTAMP COMMENT 'Timestamp when the issue was resolved. Null while open.',
  updatedAt TIMESTAMP COMMENT 'Timestamp of the last update.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (issueKey IS NOT NULL AND projectId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Jira R&D issues with flattened status, priority and assignee, and epoch milliseconds converted to timestamps.'
AS SELECT
  issueKey,
  concat('PRJ-', substring(projectKey, 3, 4)) AS projectId,
  fields.summary AS summary,
  fields.issueType.name AS issueType,
  fields.status.name AS statusName,
  fields.status.statusCategory = 'done' AS isResolved,
  fields.priority.name AS priorityName,
  fields.assignee.displayName AS assigneeName,
  fields.assignee.emailAddress AS assigneeEmail,
  fields.assignee.accountId AS assigneeAccountId,
  fields.storyPoints AS storyPoints,
  timestamp_millis(createdMs) AS createdAt,
  timestamp_millis(resolvedMs) AS resolvedAt,
  timestamp_millis(updatedMs) AS updatedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${jira_rd_bronze_schema}`.`01_issues`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${jira_rd_silver_schema}`.`02_issue_transition` (
  issueKey STRING COMMENT 'Jira issue key the transition belongs to.',
  fromStatus STRING COMMENT 'Status before the transition.',
  toStatus STRING COMMENT 'Status after the transition.',
  changedAt TIMESTAMP COMMENT 'Timestamp of the transition.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (issueKey IS NOT NULL AND changedAt IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Status transitions of Jira R&D issues with timestamps.'
AS SELECT
  issueKey,
  fromStatus,
  toStatus,
  timestamp_millis(changedAtMs) AS changedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${jira_rd_bronze_schema}`.`01_issue_transitions`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${jira_rd_silver_schema}`.`02_sprint` (
  sprintId BIGINT COMMENT 'Jira sprint identifier.',
  projectId STRING COMMENT 'Canonical project identifier of the sprint board.',
  sprintName STRING COMMENT 'Name of the sprint.',
  sprintState STRING COMMENT 'closed or active.',
  startDate DATE COMMENT 'First day of the sprint.',
  endDate DATE COMMENT 'Last day of the sprint.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (sprintId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Sprints per project from Jira R&D with dates converted from epoch milliseconds.'
AS SELECT
  CAST(sprintId AS BIGINT) AS sprintId,
  concat('PRJ-', substring(projectKey, 3, 4)) AS projectId,
  name AS sprintName,
  state AS sprintState,
  CAST(timestamp_millis(startMs) AS DATE) AS startDate,
  CAST(timestamp_millis(endMs) AS DATE) AS endDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${jira_rd_bronze_schema}`.`01_sprints`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${jira_rd_silver_schema}`.`02_worklog` (
  worklogId BIGINT COMMENT 'Jira worklog identifier.',
  issueKey STRING COMMENT 'Jira issue key the time was logged on.',
  authorName STRING COMMENT 'Display name of the person who logged the time.',
  authorEmail STRING COMMENT 'Email address of the person who logged the time.',
  hoursSpent DECIMAL(6,2) COMMENT 'Time spent in hours.',
  startedAt TIMESTAMP COMMENT 'Timestamp when the work started.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (worklogId IS NOT NULL AND issueKey IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Worklogs from Jira R&D with seconds converted to hours and flattened author.'
AS SELECT
  CAST(worklogId AS BIGINT) AS worklogId,
  issueKey,
  author.displayName AS authorName,
  author.emailAddress AS authorEmail,
  CAST(timeSpentSeconds / 3600.0 AS DECIMAL(6,2)) AS hoursSpent,
  timestamp_millis(startedMs) AS startedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${jira_rd_bronze_schema}`.`01_worklogs`);
