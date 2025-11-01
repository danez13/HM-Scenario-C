# Scenario C
## Problem framing & assumptions
### Goal
Detect, reconcile, and explain deltas between external job records and internal ledger. Additionally efficiently route fixes so that final reported revenue per month is within < 1% variance of client totals.

### Assumptions
* Client provides a JSON API per month with a list of jobs. Each job includes at minimum the following fields: `job_id`, `job_date`, `site`, `service_type`, `quantity`, `total_price`, `meta`
* Internal ledge can be exported to a CSV/DB table with at minimum the following fields: `ledger_id`, `job_date`, `units`, `total_price`, `amount`
* Site names are free text and noisy (misspellings/abbreviations). 
* No canonical site ID from client
* Job identifiers are often not shared or are non-unique between systems
* Monthly batch reconciliation
* Ingest occurs once daily and occurs monthly
* Ignore cents: variance tolerance computed at dollar amounts rounded to nearest dollar.
* Data volumes: small-to-moderate (tens of thousands of jobs a month per client)
* Solutions scaleable to larger volumes with same design

### Inputs/Outputs
* Inputs: 
  * `client_jobs.json` (API)
  * `ledger_jobs.csv` (internal)
* Outputs: 
  * Reconciliation report (CSV/DB)
  * Exception queue (records with suggested fixes)
  * Dashboard metrics (KPIs)
  * automated adjustments for low-risk items