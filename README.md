# Research and Development (rnd)

Sandbox catalog for the R&D domain, following the same pattern as the HR repo: one DAB bundle per gold data product, medallion layers inside one workspace.

## Layout

```
dab/
  _shared/        targets, schema variables, schemas, bronze generators, tagging notebook
  infrastructure/ governed tag, ABAC PII policy, schemas
  <product>/      one bundle per data product (pipeline, job, silver + gold SQL, column_metadata.sql)
```

## Data products

| Bundle | Source (bronze/silver) | Gold tables | Depends on |
|---|---|---|---|
| `research_portfolio` | planview, jira_rd | dim_program, dim_project, fact_project_milestones, agg_portfolio_status, agg_task_throughput, agg_risk_exposure | – |
| `lab_quality` | labware | fact_sample_results, agg_assay_pass_rate, agg_turnaround_time, agg_out_of_spec | – |
| `rd_financials` | – | fact_rd_spend_monthly, agg_budget_vs_actual, agg_spend_by_program, agg_resource_cost_fte | research_portfolio |
| `experimentation` | benchling | dim_experiment, fact_experiment_outcomes, agg_success_rate_by_program, agg_scientist_productivity | research_portfolio |
| `clinical_trials` | medidata_rave | dim_trial, fact_enrollment_monthly, agg_site_performance, agg_adverse_events | research_portfolio |
| `regulatory_affairs` | veeva_vault | fact_submissions, agg_approval_cycle_time, agg_document_compliance, agg_agency_query_response | – |
| `ip_portfolio` | patent_db | dim_patent, agg_patents_by_status, agg_patent_expiry_forecast, agg_inventor_activity | – |
| `equipment_utilization` | equipment_iot | fact_instrument_usage_daily, agg_utilization_by_lab, agg_downtime_events, agg_booking_demand | – |

`research_portfolio` owns the planview silver schema, so it must have run before `rd_financials`, `experimentation` and `clinical_trials`.

## Deploy and run

```
cd dab/infrastructure && databricks bundle deploy -t dev
cd ../research_portfolio && databricks bundle deploy -t dev
databricks bundle run rnd_research_portfolio -t dev --params init_bronze=true
```

Repeat for the other bundles (job name `rnd_<bundle>`). `init_bronze=true` regenerates bronze and full-refreshes the pipeline; the default (`false`) only refreshes silver and gold. `rd_financials` has no bronze and no `init_bronze` parameter.

Job flow: `[init bronze] -> pipeline refresh -> tag_tables -> column_metadata`.

## Naming and metadata

- Tables: `01_` bronze, `02_` silver, `03_` gold, `this_type_of_naming`.
- Bronze columns follow each source's own convention; silver columns are `camelCase` with typed columns and comments; gold tables and every gold column are commented.
- Each bundle owns `metadata/column_metadata.sql` (gold column comments and `pii` column tags).

## PII and ABAC

Columns carrying personal data are tagged with the governed tag `pii`. The ABAC policy in `infrastructure` masks them for everyone except the group `grp-rnd-sensitive`. The identity running the pipelines must be in that group, otherwise downstream tables read masked values.

## Not yet verified

All R&D code was written without a workspace run. Watch for: ABAC/tag-policy support in Free Edition, `SET TAG` re-runs on already tagged columns, and the `dd-MMM-yyyy` parsing in the medidata_rave silver layer.
