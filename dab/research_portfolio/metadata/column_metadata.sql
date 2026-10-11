-- Databricks notebook source
-- Column tags (pii) and descriptions owned by the research_portfolio bundle.
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:planview_silver_schema);

SET TAG ON COLUMN `02_program`.programManagerName pii;
SET TAG ON COLUMN `02_project`.projectManagerName pii;

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:jira_rd_silver_schema);

SET TAG ON COLUMN `02_issue`.assigneeName pii;
SET TAG ON COLUMN `02_issue`.assigneeEmail pii;
SET TAG ON COLUMN `02_issue`.assigneeAccountId pii;
SET TAG ON COLUMN `02_worklog`.authorName pii;
SET TAG ON COLUMN `02_worklog`.authorEmail pii;

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:research_portfolio_gold_schema);

COMMENT ON COLUMN `03_dim_program`.programId IS 'Canonical program identifier (PG-001 format).';
COMMENT ON COLUMN `03_dim_program`.programName IS 'Name of the research program.';
COMMENT ON COLUMN `03_dim_program`.therapeuticArea IS 'Therapeutic area of the program.';
COMMENT ON COLUMN `03_dim_program`.developmentPhase IS 'Current development phase.';
COMMENT ON COLUMN `03_dim_program`.programStatus IS 'Portfolio status of the program.';
COMMENT ON COLUMN `03_dim_program`.programManagerName IS 'Name of the program manager.';
COMMENT ON COLUMN `03_dim_program`.startDate IS 'Date the program started.';
COMMENT ON COLUMN `03_dim_program`.projectCount IS 'Number of projects in the program.';
COMMENT ON COLUMN `03_dim_program`.openProjectCount IS 'Number of Active, At Risk or On Hold projects.';
COMMENT ON COLUMN `03_dim_program`.totalBudgetEur IS 'Sum of project budgets in EUR.';
SET TAG ON COLUMN `03_dim_program`.programManagerName pii;

-- COMMAND ----------
COMMENT ON COLUMN `03_dim_project`.projectId IS 'Canonical project identifier (PRJ-0001 format).';
COMMENT ON COLUMN `03_dim_project`.projectName IS 'Title of the project.';
COMMENT ON COLUMN `03_dim_project`.projectType IS 'Type of the project.';
COMMENT ON COLUMN `03_dim_project`.projectStatus IS 'Status of the project.';
COMMENT ON COLUMN `03_dim_project`.priority IS 'Priority of the project.';
COMMENT ON COLUMN `03_dim_project`.programId IS 'Identifier of the parent program.';
COMMENT ON COLUMN `03_dim_project`.programName IS 'Name of the parent program.';
COMMENT ON COLUMN `03_dim_project`.therapeuticArea IS 'Therapeutic area of the parent program.';
COMMENT ON COLUMN `03_dim_project`.developmentPhase IS 'Development phase of the parent program.';
COMMENT ON COLUMN `03_dim_project`.projectManagerName IS 'Name of the project manager.';
COMMENT ON COLUMN `03_dim_project`.startDate IS 'Start date of the project.';
COMMENT ON COLUMN `03_dim_project`.plannedEndDate IS 'Planned finish date.';
COMMENT ON COLUMN `03_dim_project`.actualEndDate IS 'Actual finish date, null while not finished.';
COMMENT ON COLUMN `03_dim_project`.plannedDurationDays IS 'Days between start and actual (or planned) finish.';
COMMENT ON COLUMN `03_dim_project`.plannedBudgetEur IS 'Approved budget in EUR.';
SET TAG ON COLUMN `03_dim_project`.projectManagerName pii;

-- COMMAND ----------
COMMENT ON COLUMN `03_fact_project_milestones`.milestoneId IS 'Planview milestone identifier.';
COMMENT ON COLUMN `03_fact_project_milestones`.projectId IS 'Identifier of the project.';
COMMENT ON COLUMN `03_fact_project_milestones`.programId IS 'Identifier of the parent program.';
COMMENT ON COLUMN `03_fact_project_milestones`.programName IS 'Name of the parent program.';
COMMENT ON COLUMN `03_fact_project_milestones`.milestoneName IS 'Name of the milestone.';
COMMENT ON COLUMN `03_fact_project_milestones`.milestoneStatus IS 'Achieved or Open.';
COMMENT ON COLUMN `03_fact_project_milestones`.plannedDate IS 'Planned date of the milestone.';
COMMENT ON COLUMN `03_fact_project_milestones`.actualDate IS 'Date the milestone was achieved.';
COMMENT ON COLUMN `03_fact_project_milestones`.slipDays IS 'Days between actual and planned date; positive means late.';
COMMENT ON COLUMN `03_fact_project_milestones`.isOnTime IS 'True when achieved on or before the planned date.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_portfolio_status`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_agg_portfolio_status`.programName IS 'Name of the program.';
COMMENT ON COLUMN `03_agg_portfolio_status`.therapeuticArea IS 'Therapeutic area of the program.';
COMMENT ON COLUMN `03_agg_portfolio_status`.developmentPhase IS 'Development phase of the program.';
COMMENT ON COLUMN `03_agg_portfolio_status`.projectCount IS 'Total number of projects.';
COMMENT ON COLUMN `03_agg_portfolio_status`.activeProjects IS 'Projects with status Active.';
COMMENT ON COLUMN `03_agg_portfolio_status`.atRiskProjects IS 'Projects with status At Risk.';
COMMENT ON COLUMN `03_agg_portfolio_status`.onHoldProjects IS 'Projects with status On Hold.';
COMMENT ON COLUMN `03_agg_portfolio_status`.completedProjects IS 'Projects with status Completed.';
COMMENT ON COLUMN `03_agg_portfolio_status`.cancelledProjects IS 'Projects with status Cancelled.';
COMMENT ON COLUMN `03_agg_portfolio_status`.totalBudgetEur IS 'Sum of project budgets in EUR.';
COMMENT ON COLUMN `03_agg_portfolio_status`.achievedMilestones IS 'Number of achieved milestones.';
COMMENT ON COLUMN `03_agg_portfolio_status`.avgMilestoneSlipDays IS 'Average slip in days of achieved milestones.';
COMMENT ON COLUMN `03_agg_portfolio_status`.milestoneOnTimeRate IS 'Share of achieved milestones delivered on time (0-1).';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_task_throughput`.projectId IS 'Identifier of the project.';
COMMENT ON COLUMN `03_agg_task_throughput`.monthStart IS 'First day of the month.';
COMMENT ON COLUMN `03_agg_task_throughput`.programId IS 'Identifier of the parent program.';
COMMENT ON COLUMN `03_agg_task_throughput`.programName IS 'Name of the parent program.';
COMMENT ON COLUMN `03_agg_task_throughput`.issuesCreated IS 'Issues created in the month.';
COMMENT ON COLUMN `03_agg_task_throughput`.issuesResolved IS 'Issues resolved in the month.';
COMMENT ON COLUMN `03_agg_task_throughput`.avgCycleTimeDays IS 'Average days from creation to resolution for issues resolved in the month.';
COMMENT ON COLUMN `03_agg_task_throughput`.storyPointsDelivered IS 'Story points of issues resolved in the month.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_risk_exposure`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_agg_risk_exposure`.programName IS 'Name of the program.';
COMMENT ON COLUMN `03_agg_risk_exposure`.openRisks IS 'Number of open risks.';
COMMENT ON COLUMN `03_agg_risk_exposure`.highRisks IS 'Open risks with a score of 15 or more.';
COMMENT ON COLUMN `03_agg_risk_exposure`.avgRiskScore IS 'Average score (probability x impact) of open risks.';
COMMENT ON COLUMN `03_agg_risk_exposure`.maxRiskScore IS 'Highest score among open risks.';
