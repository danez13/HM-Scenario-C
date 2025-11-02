# Architecture - Revenue Reconciliation & Anomaly Detection
## Overview
This document describes the high-level architecture for the **Revenue Reconciliation & Anomaly Detection System**. The system ingests client job data (via API) and internal ledger data, performs deterministic and fuzzy matching, detects differences, and routes exceptions for human review. The system ensures monthly revenue variance remains under **1%**.

### Design Goals
* End-to-end automated reconciliation with explainable mismatches
* <1% total revenue variance tolerance (ignoring cents)
* Clear separation of ingestion, matching, classification, and review
* Human-in-the-loop (HIL) review for low-confidence or high impact anomalies
* Full auditability and data lineage

## High-level System Diagram
This diagram illustrates the **end-to-end business process**, from data ingestion to human review. It focuses on *what happens* rather than *how it is implemented*.

![flow chart](/docs/highLevelSystem/hlsd.png)

#### Flow Chart Summary
1. **Client APi & Ledger Import:** Fetch monthly job and ledger exports
2. **Normalization:** Clean and standardize fields (dates, sites, service types)
3. **Matching Enginer:** Deterministic (exact key) and fuzzy (similarity-based) matching
4. **Difference & Classification:** Compute differences and assign root-cause categories.
5. **Anomaly detection:** Auto-resolve low-impact items; route exceptions to review queue
6. **Review UI:** Finance/Ops users validate or override suggested fixes.
7. **Metrics & Audit:** Variance KPIs, alerts, and immutable reconciliation

## Layered Architecture
This sequence diagram traces a **single reconciliation run** (e.g., one batch cycle). It shows temporal interactions between the scheduler, ingestion service, matching engine, UI.

![layered architecture diagram](/docs/layeredArchitecture/LayeredArchitecture.png)

### Layered Architecture Summary

| Layer | Purpose | Example Components | Key Responsibilities | Key Outputs |
| --- | --- | --- | --- | --- |
| Application | review & reporting | Streamlit UI, dashboards | Display results, manage review queue | Validated reconciliation, reports |
| Business Logic | Reconciliation & anomaly detection | Matching engine, detection | Match records, calculate differences, classify issues | differences, root-cause tags |
| Data | Ingest & store structured data | API fetchers, normalized tables | Clean dataset for analysis | Clean data for analysis |
| Infrastructure | Support runtime and scalability | Scheduler,cloud storage | Job orchestration, resilience, logging | Logs, reliability metrics |

### Component Interactions
* **Scheduler** Triggers periodic ingestion and reconciliation jobs.
* **Data Layer** fetches and normalizes client and ledger data.
* **Business Logic Layer** performs deterministic/fuzzy matching and anomaly detection.
* **Application Layer** exposes reviewed results and variance metrics via UI
* **Infrastructure Layer** ensures reliability through retries, logging, and monitoring.

## Sequence
![Sequence diagram](/docs/sequence/Sequence.png)

## Data Flow
![Data Flow Summary](/docs/dataFlow/DataFlow.png)

### Data Flow Summary

| Step | Description |
| --- | --- |
| Ingestion | Scheduled import of both data sources
| Normalization | Standardize dates, text, numeric formats |
| Matching | Deterministic + fuzzy join |
| difference Calculation | Amount and percentage variance per job |
| Classification | Rule-based taxonomy (rate change, duplicate, etc.) |
| Anomaly Handling | Auto-resolve or assign to reviewer |
| Review & Reporting | Finance/Ops validation and KPI summary |

## Scalability & Resilience
| Concern | Design Consideration |
| --- | --- |
|Data Volume | Matching engine supports batching or DB joins; scalable to 100k+ jobs/month |
| Error Handling | Failed ingestion or parsing triggers alert; partial runs retryable |
| Extensibility | Modular functions: add new classifiers or matching heuristics easily |
| Auditability | Every transformation logged with timestamp and source identifiers |
| Monitoring | Daily variance KPI, DQ checks, Slack/email alerts for >1% deviation |

## Security & Configuration
* Secrets (API keys, DB creds) stored in `.env` or cloud secrets manager
* `.env.example` provided for local setup
* Personally identifiable information (PII) excluded from exports
* Access control: Finance and Ops reviewers authenticated in UI

## Future Enhancements
* Ml-based matching models (pairwise classifier for similarity scoring)
* Active-learning feedback loop from reviewers
* Integration with Finance ERP for automatic journal entries
* Rate-change trend detection & alerting
* Centralized metrics dashboard (Grafana/Metabase)

---

*Last updated: 11/2/2025*