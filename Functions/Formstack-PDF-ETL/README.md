# Formstack PDF ETL

Azure Function that ingests PDF submissions from Formstack, extracts and fills form fields, and writes the results to Blob Storage. Generates a SAS-secured link for downstream access.

**Trigger:** HTTP
**Auth:** Azure Key Vault (Managed Identity / Client Secret depending on environment)
**Output:** Processed PDFs written to Blob Storage
