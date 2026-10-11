-- Databricks notebook source
-- Column tags and descriptions owned by the ip_portfolio bundle.
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:patent_db_silver_schema);

SET TAG ON COLUMN `02_inventor`.inventorName pii;

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:ip_portfolio_gold_schema);

SET TAG ON COLUMN `03_agg_inventor_activity`.inventorName pii;

COMMENT ON COLUMN `03_dim_patent`.patentId IS 'Patent application identifier.';
COMMENT ON COLUMN `03_dim_patent`.applicationNumber IS 'Application number assigned by the patent office.';
COMMENT ON COLUMN `03_dim_patent`.jurisdiction IS 'Filing jurisdiction code.';
COMMENT ON COLUMN `03_dim_patent`.legalStatus IS 'Granted, Pending, Abandoned, Lapsed or Withdrawn.';
COMMENT ON COLUMN `03_dim_patent`.filingDate IS 'Date the application was filed.';
COMMENT ON COLUMN `03_dim_patent`.grantDate IS 'Date the patent was granted.';
COMMENT ON COLUMN `03_dim_patent`.estimatedExpiryDate IS 'Estimated expiry, 20 years after filing.';
COMMENT ON COLUMN `03_dim_patent`.claimCount IS 'Number of claims.';
COMMENT ON COLUMN `03_dim_patent`.annualFeeUsd IS 'Annual maintenance fee in USD.';
COMMENT ON COLUMN `03_dim_patent`.familyId IS 'Invention family identifier.';
COMMENT ON COLUMN `03_dim_patent`.familyTitle IS 'Invention title.';
COMMENT ON COLUMN `03_dim_patent`.techCategory IS 'Technology category.';
COMMENT ON COLUMN `03_dim_patent`.programId IS 'Identifier of the related program.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_patents_by_status`.programId IS 'Identifier of the related program.';
COMMENT ON COLUMN `03_agg_patents_by_status`.jurisdiction IS 'Filing jurisdiction code.';
COMMENT ON COLUMN `03_agg_patents_by_status`.legalStatus IS 'Legal status.';
COMMENT ON COLUMN `03_agg_patents_by_status`.patentCount IS 'Number of patents.';
COMMENT ON COLUMN `03_agg_patents_by_status`.annualFeeUsd IS 'Total annual fees in USD.';
COMMENT ON COLUMN `03_agg_patents_by_status`.avgClaimCount IS 'Average number of claims.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_patent_expiry_forecast`.expiryYear IS 'Year of estimated expiry.';
COMMENT ON COLUMN `03_agg_patent_expiry_forecast`.programId IS 'Identifier of the related program.';
COMMENT ON COLUMN `03_agg_patent_expiry_forecast`.expiringPatentCount IS 'Granted or pending patents expiring in the year.';
COMMENT ON COLUMN `03_agg_patent_expiry_forecast`.annualFeeUsdReleased IS 'Annual fees in USD that fall away after expiry.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_inventor_activity`.inventorId IS 'Inventor identifier.';
COMMENT ON COLUMN `03_agg_inventor_activity`.inventorName IS 'Inventor full name. Personal data.';
COMMENT ON COLUMN `03_agg_inventor_activity`.patentCount IS 'Number of patents the inventor is named on.';
COMMENT ON COLUMN `03_agg_inventor_activity`.leadRoleCount IS 'Patents where the inventor is the lead.';
COMMENT ON COLUMN `03_agg_inventor_activity`.grantedCount IS 'Granted patents.';
COMMENT ON COLUMN `03_agg_inventor_activity`.firstFilingDate IS 'Earliest filing date among the inventor patents.';
COMMENT ON COLUMN `03_agg_inventor_activity`.latestFilingDate IS 'Latest filing date among the inventor patents.';
