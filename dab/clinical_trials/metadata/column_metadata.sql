-- Databricks notebook source
-- Column tags (pii) and descriptions owned by the clinical_trials bundle.
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:medidata_rave_silver_schema);

SET TAG ON COLUMN `02_site`.principalInvestigatorName pii;
SET TAG ON COLUMN `02_subject`.subjectId pii;
SET TAG ON COLUMN `02_subject`.birthYear pii;
SET TAG ON COLUMN `02_subject`.sex pii;
SET TAG ON COLUMN `02_adverse_event`.subjectId pii;

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:clinical_trials_gold_schema);

COMMENT ON COLUMN `03_dim_trial`.trialId IS 'Study identifier.';
COMMENT ON COLUMN `03_dim_trial`.trialTitle IS 'Protocol title.';
COMMENT ON COLUMN `03_dim_trial`.phase IS 'Clinical phase.';
COMMENT ON COLUMN `03_dim_trial`.trialStatus IS 'Ongoing, Recruiting or Completed.';
COMMENT ON COLUMN `03_dim_trial`.startDate IS 'Date the trial started.';
COMMENT ON COLUMN `03_dim_trial`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_dim_trial`.programName IS 'Name of the program.';
COMMENT ON COLUMN `03_dim_trial`.therapeuticArea IS 'Therapeutic area of the program.';
COMMENT ON COLUMN `03_dim_trial`.siteCount IS 'Number of sites.';
COMMENT ON COLUMN `03_dim_trial`.subjectCount IS 'Number of enrolled subjects.';

-- COMMAND ----------
COMMENT ON COLUMN `03_fact_enrollment_monthly`.trialId IS 'Study identifier.';
COMMENT ON COLUMN `03_fact_enrollment_monthly`.siteId IS 'Site identifier.';
COMMENT ON COLUMN `03_fact_enrollment_monthly`.monthStart IS 'First day of the enrollment month.';
COMMENT ON COLUMN `03_fact_enrollment_monthly`.subjectsEnrolled IS 'Subjects enrolled in the month.';
COMMENT ON COLUMN `03_fact_enrollment_monthly`.treatmentSubjects IS 'Subjects enrolled in the Treatment arm.';
COMMENT ON COLUMN `03_fact_enrollment_monthly`.placeboSubjects IS 'Subjects enrolled in the Placebo arm.';
COMMENT ON COLUMN `03_fact_enrollment_monthly`.cumulativeTrialEnrollment IS 'Cumulative subjects enrolled in the trial up to and including the month.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_site_performance`.siteId IS 'Site identifier.';
COMMENT ON COLUMN `03_agg_site_performance`.trialId IS 'Study identifier.';
COMMENT ON COLUMN `03_agg_site_performance`.siteName IS 'Name of the site.';
COMMENT ON COLUMN `03_agg_site_performance`.countryCode IS 'ISO country code of the site.';
COMMENT ON COLUMN `03_agg_site_performance`.principalInvestigatorName IS 'Name of the principal investigator.';
COMMENT ON COLUMN `03_agg_site_performance`.subjectsEnrolled IS 'Subjects enrolled at the site.';
COMMENT ON COLUMN `03_agg_site_performance`.firstEnrollmentDate IS 'Date of the first enrollment at the site.';
COMMENT ON COLUMN `03_agg_site_performance`.adverseEvents IS 'Adverse events reported at the site.';
COMMENT ON COLUMN `03_agg_site_performance`.seriousEvents IS 'Serious adverse events reported at the site.';
COMMENT ON COLUMN `03_agg_site_performance`.adverseEventsPerSubject IS 'Adverse events per enrolled subject.';
SET TAG ON COLUMN `03_agg_site_performance`.principalInvestigatorName pii;

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_adverse_events`.trialId IS 'Study identifier.';
COMMENT ON COLUMN `03_agg_adverse_events`.eventTerm IS 'Adverse event term.';
COMMENT ON COLUMN `03_agg_adverse_events`.severity IS 'Mild, Moderate or Severe.';
COMMENT ON COLUMN `03_agg_adverse_events`.eventCount IS 'Number of events.';
COMMENT ON COLUMN `03_agg_adverse_events`.seriousEventCount IS 'Number of serious events.';
COMMENT ON COLUMN `03_agg_adverse_events`.affectedSubjects IS 'Number of distinct subjects with the event.';
