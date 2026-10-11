-- Gold data product: IP portfolio. Materialized views over the patent database silver tables.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${ip_portfolio_gold_schema}`.`03_dim_patent`
COMMENT 'Patent dimension: one row per patent application with family, program, status and estimated expiry (20 years after filing).'
AS
SELECT
  p.patentId,
  p.applicationNumber,
  p.jurisdiction,
  p.legalStatus,
  p.filingDate,
  p.grantDate,
  add_months(p.filingDate, 240) AS estimatedExpiryDate,
  p.claimCount,
  p.annualFeeUsd,
  f.familyId,
  f.title AS familyTitle,
  f.techCategory,
  f.programId
FROM `${catalog}`.`${patent_db_silver_schema}`.`02_patent` p
LEFT JOIN `${catalog}`.`${patent_db_silver_schema}`.`02_patent_family` f ON f.familyId = p.familyId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${ip_portfolio_gold_schema}`.`03_agg_patents_by_status`
COMMENT 'Patent counts and annual fees by program, jurisdiction and legal status.'
AS
SELECT
  programId,
  jurisdiction,
  legalStatus,
  count(*) AS patentCount,
  sum(annualFeeUsd) AS annualFeeUsd,
  round(avg(claimCount), 1) AS avgClaimCount
FROM `${catalog}`.`${ip_portfolio_gold_schema}`.`03_dim_patent`
GROUP BY programId, jurisdiction, legalStatus;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${ip_portfolio_gold_schema}`.`03_agg_patent_expiry_forecast`
COMMENT 'Forecast of live patents reaching their estimated expiry per year and program, with the annual fees that fall away.'
AS
SELECT
  year(estimatedExpiryDate) AS expiryYear,
  programId,
  count(*) AS expiringPatentCount,
  sum(annualFeeUsd) AS annualFeeUsdReleased
FROM `${catalog}`.`${ip_portfolio_gold_schema}`.`03_dim_patent`
WHERE legalStatus IN ('Granted', 'Pending')
GROUP BY year(estimatedExpiryDate), programId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${ip_portfolio_gold_schema}`.`03_agg_inventor_activity`
COMMENT 'Inventor activity: patents, lead roles and granted patents per inventor.'
AS
SELECT
  i.inventorId,
  i.inventorName,
  count(DISTINCT i.patentId) AS patentCount,
  count_if(i.role = 'Lead') AS leadRoleCount,
  count_if(d.legalStatus = 'Granted') AS grantedCount,
  min(d.filingDate) AS firstFilingDate,
  max(d.filingDate) AS latestFilingDate
FROM `${catalog}`.`${patent_db_silver_schema}`.`02_inventor` i
JOIN `${catalog}`.`${ip_portfolio_gold_schema}`.`03_dim_patent` d ON d.patentId = i.patentId
GROUP BY i.inventorId, i.inventorName;
