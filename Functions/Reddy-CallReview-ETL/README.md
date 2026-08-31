# Reddy Call Review ETL

Azure Function that pulls call recordings and metadata from the CXone API, processes them for call review, and writes results to Blob Storage.

**Trigger:** HTTP
**Auth:** CXone OAuth token + Azure Key Vault for storage credentials
**Output:** Processed call review data in Blob Storage
