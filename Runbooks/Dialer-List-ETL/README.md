# Dialer List ETL

Azure Automation Runbook that authenticates with the CXone API, pulls dialer list data, and writes processed output to Blob Storage.

**Trigger:** Scheduled (Azure Automation)
**Auth:** CXone OAuth token + Managed Identity for storage credentials
**Output:** Processed dialer list in Blob Storage
