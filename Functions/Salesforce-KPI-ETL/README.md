# Salesforce KPI ETL

Azure Function that processes Salesforce export data into a full user/date grid and computes daily KPI metrics for reporting.

**Trigger:** HTTP
**Auth:** Azure Key Vault (Managed Identity / Client Secret depending on environment)
**Output:** KPI dataset written to Blob Storage for Power BI consumption
