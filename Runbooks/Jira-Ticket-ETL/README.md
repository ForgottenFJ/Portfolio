# Jira Ticket ETL

Azure Automation Runbook that pulls Jira tickets via the REST API, transforms them into a reporting-ready format, and writes to Blob Storage.

**Trigger:** Scheduled (Azure Automation)
**Auth:** Jira Basic Auth + Managed Identity for storage credentials
**Output:** Processed ticket data in Blob Storage
