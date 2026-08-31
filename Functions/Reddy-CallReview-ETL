import requests, logging, os, base64, json, io, gc, time, math, mimetypes
import pandas as pd
from io import StringIO, BytesIO
import azure.functions as func
from azure.identity import EnvironmentCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.getLogger("azure").setLevel(logging.WARNING)

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


# ─────────────────────────────────────────────────────────────────────────────
# Azure / Key Vault setup
# ─────────────────────────────────────────────────────────────────────────────
def get_blob_service_client():
    azure_vault_url = os.environ["azure_vault_url"]

    if os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT") == "Development":
        logging.info("Using EnvironmentCredential for local development.")
        credential = EnvironmentCredential()
    else:
        logging.info("Using ClientSecretCredential for Azure environment.")
        credential = ClientSecretCredential(
            os.environ["azure_tenant_id"],
            os.environ["azure_client_id"],
            os.environ["azure_client_secret"],
        )

    client = SecretClient(vault_url=azure_vault_url, credential=credential)
    connection_string = client.get_secret("azure-connection-string").value
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return blob_service_client, client


blob_service_client, client = get_blob_service_client()

cxone_client_id     = client.get_secret("cxone-client-id").value
cxone_client_secret = client.get_secret("cxone-client-secret").value
cxone_access_key    = client.get_secret("cxone-access-key").value
cxone_access_secret = client.get_secret("cxone-access-secret").value

director_title   = "Direct_Title"
DIRECTOR_EMAILS = ["director1@example.com", "director2@example.com"]
REDDY_PRODUCT_ID = int(os.environ["PRODUCT_ID"])

EXTRACT_BASE  = "https://api-na1.niceincontact.com/data-extraction/v1/jobs"
SEGMENT_BASE  = "https://api-na1.niceincontact.com/media-playback/v1/segments"
POLL_INTERVAL = 5
POLL_TIMEOUT  = 300

EXCLUDE_DISPOSITIONS = [
    "Disposition A",
    "Disposition B",
    "Transferred",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def save_csv(container_client, df: pd.DataFrame, path: str):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, float_format="%.2f")
    csv_data = csv_buffer.getvalue().encode("utf-8-sig")
    container_client.get_blob_client(path).upload_blob(csv_data, overwrite=True)
    logging.info(f"Saved CSV to {path} with {len(df)} rows")
    del csv_buffer, csv_data
    gc.collect()


def cxone_bearer_token() -> dict:
    credentials = f"{cxone_client_id}:{cxone_client_secret}"
    encoded     = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    response = requests.post(
        "https://cxone.niceincontact.com/auth/token",
        headers={
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "password",
            "username": cxone_access_key,
            "password": cxone_access_secret,
        },
    )

    if response.status_code != 200:
        logging.error(f"Auth failed: {response.status_code} — {response.text}")
        raise RuntimeError("CXone authentication failed")

    data = response.json()
    logging.info("CXone API Token Obtained")

    return {"Authorization": f"{data.get('token_type', 'Bearer')} {data['access_token']}"}

def sanitize_for_json(obj):
    """Recursively replace non-JSON-compliant floats with None."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    elif hasattr(obj, 'item'):  # numpy scalar
        return sanitize_for_json(obj.item())
    return obj


# ─────────────────────────────────────────────────────────────────────────────
# Salesforce / hierarchy loaders
# ─────────────────────────────────────────────────────────────────────────────

def get_case_consultants_under_director(users: pd.DataFrame, director_emails: list[str]) -> pd.DataFrame:
    users = users.copy()
    users["Email_lower"]         = users["Email"].str.lower()
    users["Manager_Email_lower"] = users["Manager Email"].str.lower()

    director_emails_lower = [e.lower() for e in director_emails]
    missing = [e for e in director_emails_lower if e not in users["Email_lower"].values]
    if missing:
        logging.warning(f"Director emails not found in user table: {missing}")

    manager_to_reports = users.groupby("Manager_Email_lower")["Email_lower"].apply(list).to_dict()

    def get_all_subordinate_emails(manager_email: str, visited: set = None) -> set:
        if visited is None:
            visited = set()
        if manager_email in visited:
            return set()
        visited.add(manager_email)
        subordinate_emails = set()
        for report_email in manager_to_reports.get(manager_email, []):
            subordinate_emails.add(report_email)
            subordinate_emails.update(get_all_subordinate_emails(report_email, visited))
        return subordinate_emails

    all_subordinate_emails = set()
    for email in director_emails_lower:
        all_subordinate_emails.update(get_all_subordinate_emails(email))

    case_consultants = users[
        (users["Email_lower"].isin(all_subordinate_emails))
        & (users["Title"].str.contains("Case Consultant", case=False, na=False))
        & (~users["LastName"].str.contains(r"\(Inactive\)", case=False, na=False))
    ].copy()

    return case_consultants.drop(columns=["Email_lower", "Manager_Email_lower"])

def load_case_consultants_for_director(blob_service, director_emails: list[str]) -> pd.DataFrame:
    users_container = blob_service.get_container_client("master-file")

    sf_users_data = users_container.get_blob_client("sf_users.csv").download_blob().readall()
    sf_users      = pd.read_csv(io.BytesIO(sf_users_data), encoding="utf-8-sig")
    logging.info(f"Loaded {len(sf_users)} users from sf_users.csv")

    GOA_DEPARTMENTS = ["goa - benefits", "goa"]

    goa_consultants = sf_users[
        sf_users["SF_Department"].str.strip().str.lower().isin(GOA_DEPARTMENTS)
        & (~sf_users["LastName"].str.contains(r"\(Inactive\)", case=False, na=False))
    ].copy()
    logging.info(f"GOA users: {len(goa_consultants)}")

    goa_consultants = goa_consultants.rename(columns={"Id": "Intake_Agent_Name"})
    case_consultants = goa_consultants[["Email", "FirstName", "LastName", "Title", "Intake_Agent_Name"]].drop_duplicates(subset=["Intake_Agent_Name"])
    logging.info(f"Total case consultants (GOA only): {len(case_consultants)}")
    return case_consultants

# ─────────────────────────────────────────────────────────────────────────────
# get_sf_intakes  —  canonical agent field = Intake_Agent_Name__c fillna OwnerId
# ─────────────────────────────────────────────────────────────────────────────
def get_sf_intakes() -> pd.DataFrame:
    logging.info("Downloading Salesforce Intake data from blob...")
    master_container = blob_service_client.get_container_client("master-file")

    CASE_TYPES  = ["Slip and Fall", "Trip and Fall", "Automobile Accident", "Automobile Accidents", "Social Security"]
    LITIGATIONS = ["Personal Injury", "Premises Liability", "Social Security"]

    raw = master_container.get_blob_client(
        "Salesforce/Intake/intake_reddy.csv"
    ).download_blob().readall().decode("utf-8-sig")
    df = pd.read_csv(
        io.StringIO(raw),
        low_memory=False,
        dtype={"Id": str},
    )
    logging.info(f"Loaded {len(df)} records from intake_reddy.csv")

    before = len(df)
    df = df[df["Id"].notna() & df["Id"].str.strip().ne("")]
    df = df[df["Id"].str.len().isin([15, 18])]
    if before != len(df):
        logging.warning(f"Dropped {before - len(df)} rows with blank/malformed Id")

    df = df[df["Id"].notna()]
    dupes = df[df.duplicated("Id", keep=False)]
    if not dupes.empty:
        logging.warning(f"{dupes['Id'].nunique()} IDs have multiple rows; collapsing with first()")
    df = df.groupby("Id", sort=False).first().reset_index()

    if "CreatedDate" in df.columns:
        df["CreatedDate"] = pd.to_datetime(
            df["CreatedDate"].astype(str).str.slice(0, 19),
            format="%Y-%m-%d %H:%M:%S", errors="coerce", utc=True,
        )

    df = df[
        df["Litigation__c"].isin(LITIGATIONS)
        & df["Case_Type__c"].isin(CASE_TYPES)
    ].copy()
    logging.info(f"After litigation + case type filter: {len(df)}")

    df = df.dropna(subset=["Id", "CreatedDate"]).drop_duplicates(subset=["Id"])
    logging.info(f"After dropna + dedup: {len(df)}")

    all_null_cols = df.columns[df.isna().all()].tolist()
    if all_null_cols:
        logging.warning(f"Dropping all-null columns: {all_null_cols}")
    df = df.drop(columns=all_null_cols).reset_index(drop=True)

    # CANONICAL AGENT FIELD: Intake_Agent_Name__c, fallback to OwnerId.
    # Used consistently everywhere the intake's own agent is referenced.
    df["Intake_Agent_Name"] = df["Intake_Agent_Name__c"].fillna(df["OwnerId"])

    df = df.rename(columns={"Id": "IntakeId"})
    logging.info(f"Intakes after filter: {len(df)}")
    logging.info(f"Intake_Agent_Name nulls (should be 0): {df['Intake_Agent_Name'].isna().sum()}")
    return df

def load_salesforce_calldata(blob_service) -> pd.DataFrame:
    container_client = blob_service.get_container_client("master-file")
    sf_data          = container_client.get_blob_client("Salesforce/CallData/calldata.csv").download_blob().readall()

    calldata = pd.read_csv(io.BytesIO(sf_data), on_bad_lines="skip", dtype={"contactId__c": str, "masterContactId__c": str})
    calldata = calldata.rename(columns={
        "contactId__c":               "ContactId",
        "masterContactId__c":         "masterContactId",
        "DispositionName__c":         "DispositionName",
        "dispositionNotes__c":        "DispositionNotes",
        "Intake__c":                  "IntakeId",
        "skillName__c":               "SkillName",
        "Full_Transcription_Text__c": "Transcription",
        "Owner__c":                   "OwnerId",
    })

    calldata["ContactId"]       = pd.to_numeric(calldata["ContactId"], errors="coerce").astype("Int64")
    calldata["masterContactId"] = pd.to_numeric(calldata["masterContactId"], errors="coerce").astype("Int64")
    calldata["CreatedDate"] = pd.to_datetime(calldata["CreatedDate"], errors="coerce")

    valid = calldata["CreatedDate"].notna().sum()
    total = len(calldata)
    if valid == 0:
        logging.warning(f"CreatedDate: 0/{total} rows parsed — column is corrupted. Date range will use intake dates.")
    else:
        logging.info(f"CreatedDate: {valid}/{total} rows parsed successfully")

    logging.info(f"masterContactId populated: {calldata['masterContactId'].notna().sum()}/{total}")
    logging.info(f"Loaded {total} call records")
    return calldata


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Data Extraction: build ContactId → SegmentId lookup
# ─────────────────────────────────────────────────────────────────────────────

def run_extraction_job(start_date: str, end_date: str) -> pd.DataFrame:
    logging.info(f"Submitting Data Extraction job: {start_date} → {end_date}")
    headers = cxone_bearer_token()

    response = requests.post(
        EXTRACT_BASE,
        headers=headers,
        json={
            "entityName": "recording-interaction-metadata",
            "version":    "10",
            "startDate":  start_date,
            "endDate":    end_date,
        },
    )
    if response.status_code != 202:
        raise RuntimeError(f"Job submission failed: {response.status_code} — {response.text}")

    job_id = response.json() if isinstance(response.json(), str) else response.json().get("jobId")
    logging.info(f"✓ Extraction job submitted — jobId: {job_id}")

    headers  = cxone_bearer_token()
    elapsed  = 0
    poll_url = f"{EXTRACT_BASE}/{job_id}"

    while elapsed < POLL_TIMEOUT:
        poll_resp = requests.get(poll_url, headers=headers)
        if poll_resp.status_code != 200:
            raise RuntimeError(f"Poll failed: {poll_resp.status_code} — {poll_resp.text}")

        body   = poll_resp.json()
        status = body.get("jobStatus", {}).get("status", "UNKNOWN")
        logging.info(f"  Extraction job status: {status} ({elapsed}s elapsed)")

        if status == "SUCCEEDED":
            s3_url = body["jobStatus"]["result"]["url"]
            logging.info("✓ Extraction job succeeded")
            break

        if status in ("FAILED", "CANCELLED", "EXPIRED"):
            error = body.get("jobStatus", {}).get("result", {}).get("errorMessage", "No details")
            raise RuntimeError(f"Extraction job {status}: {error}")

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    else:
        raise TimeoutError(f"Extraction job did not complete within {POLL_TIMEOUT}s")

    logging.info("Downloading extraction CSV from S3...")
    dl = requests.get(s3_url, timeout=30)
    if dl.status_code != 200:
        raise RuntimeError(f"CSV download failed: {dl.status_code}")

    df = pd.read_csv(StringIO(dl.content.decode("utf-8-sig")), low_memory=False)
    logging.info(f"✓ Extraction CSV downloaded: {len(df)} rows")
    return df


def build_segment_lookup(extraction_df: pd.DataFrame, contact_ids: pd.Series) -> dict:
    agent_contact_col = next(
        (c for c in extraction_df.columns if "Agent 1 Contact Number" in c), None
    )
    segment_col = next(
        (c for c in extraction_df.columns if c.strip() == "Segment ID"), None
    )

    if not agent_contact_col or not segment_col:
        raise RuntimeError(
            f"Required columns not found in extraction CSV. "
            f"Available: {list(extraction_df.columns)}"
        )

    extraction_df[agent_contact_col] = (pd.to_numeric(extraction_df[agent_contact_col], errors="coerce").astype("Int64"))
    contact_ids_numeric              = pd.to_numeric(contact_ids, errors="coerce")

    logging.info(f"ContactId sample (from calldata):         {contact_ids_numeric.dropna().head(5).tolist()}")
    logging.info(f"Agent 1 Contact Number sample (CXone):    {extraction_df[agent_contact_col].dropna().head(5).tolist()}")

    matched = extraction_df[extraction_df[agent_contact_col].isin(contact_ids_numeric)][
        [agent_contact_col, segment_col]
    ].drop_duplicates(subset=[agent_contact_col])

    lookup = dict(zip(
        matched[agent_contact_col].astype("Int64"),
        matched[segment_col],
    ))

    logging.info(
        f"Segment lookup built: {len(lookup)} matched out of {len(contact_ids_numeric.dropna())} contact IDs"
    )
    return lookup


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Download segment audio
# ─────────────────────────────────────────────────────────────────────────────

def download_segment_recording(segment_id: str, media_type: str = "voice-only") -> BytesIO | None:
    logging.info(f"Fetching segment: {segment_id}")

    headers = cxone_bearer_token()
    headers["accept"] = "application/json"

    params = {
        "media-type":            media_type,
        "exclude-waveforms":     "true",
        "exclude-qm-categories": "true",
    }

    try:
        response = requests.get(
            f"{SEGMENT_BASE}/{segment_id}",
            headers=headers,
            params=params,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()

        redirect_url = data.get("redirectUrl") or data.get("fileToPlayUrl")

        if not redirect_url:
            interactions = data.get("interactions", [])
            if interactions:
                redirect_url = interactions[0].get("data", {}).get("fileToPlayUrl")

        if not redirect_url:
            logging.error(f"No download URL found for segment {segment_id}: {data}")
            return None

        logging.info("✓ Got segment download URL")

    except Exception as e:
        logging.error(f"Segment metadata fetch failed for {segment_id}: {e}")
        return None

    try:
        dl = requests.get(redirect_url, stream=True, timeout=60)
        dl.raise_for_status()

        buffer = BytesIO()
        for chunk in dl.iter_content(chunk_size=8192):
            if chunk:
                buffer.write(chunk)
        buffer.seek(0)
        logging.info(f"✓ Segment audio downloaded ({buffer.getbuffer().nbytes / 1024:.1f} KB)")
        return buffer

    except Exception as e:
        logging.error(f"Segment audio download failed for {segment_id}: {e}")
        return None

def download_contact_recording(master_contact_id: str, media_type: str = "voice-only") -> BytesIO | None:
    logging.info(f"Fetching contact recording: {master_contact_id}")

    headers = cxone_bearer_token()
    headers["accept"] = "application/json"

    params = {
        "acd-call-id":           master_contact_id,
        "media-type":            media_type,
        "exclude-waveforms":     "true",
        "exclude-qm-categories": "true",
    }

    try:
        response = requests.get(
            "https://api-na1.niceincontact.com/media-playback/v1/contacts",
            headers=headers,
            params=params,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()

        redirect_url = None
        for interaction in data.get("interactions", []):
            redirect_url = interaction.get("data", {}).get("fileToPlayUrl")
            if redirect_url:
                break

        if not redirect_url:
            logging.error(f"No fileToPlayUrl found for masterContactId={master_contact_id}")
            return None

        logging.info("✓ Got contact download URL")

    except Exception as e:
        logging.error(f"Contact metadata fetch failed for {master_contact_id}: {e}")
        return None

    try:
        dl = requests.get(redirect_url, stream=True, timeout=60)
        dl.raise_for_status()

        buffer = BytesIO()
        for chunk in dl.iter_content(chunk_size=8192):
            if chunk:
                buffer.write(chunk)
        buffer.seek(0)
        logging.info(f"✓ Contact audio downloaded ({buffer.getbuffer().nbytes / 1024:.1f} KB)")
        return buffer

    except Exception as e:
        logging.error(f"Contact audio download failed for {master_contact_id}: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Reddy upload
# ─────────────────────────────────────────────────────────────────────────────

def parse_transcript(transcript_text: str) -> list:
    lines        = []
    current_time = 0

    for line in transcript_text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith("agent:"):
            role, text = "agent", line[len("agent:"):].strip()
        elif line.lower().startswith("customer:"):
            role, text = "customer", line[len("customer:"):].strip()
        else:
            continue

        word_count = len(text.split())
        duration   = max(1, round(word_count * 0.5))
        lines.append({"role": role, "text": text, "start": current_time, "stop": current_time + duration})
        current_time += duration + 1

    return lines


def upload_call_to_reddy(row: pd.Series, product_id: int) -> dict:
    reddy_api_token = os.environ["reddy_api_key"]
    headers         = {
        "Authorization": f"Bearer {reddy_api_token}",
        "Content-Type":  "application/json",
        "X-Use-K8s":     "true",
    }

    tags = []
    for col in row.index:
        val = row[col]
        if col in ("Transcription", "SegmentId"):
            continue
        if val is None:
            continue
        try:
            if pd.isna(val):
                continue
        except (TypeError, ValueError):
            pass

        clean_key = col.replace("__c", "").replace("__", "_").strip("_")

        if isinstance(val, bool):
            tags.append({"key": clean_key, "value": val, "type": "boolean"})
        elif isinstance(val, (int, float)) and not isinstance(val, bool):
            if math.isnan(val) or math.isinf(val):
                continue
            try:
                tags.append({
                    "key": clean_key,
                    "value": int(val) if float(val).is_integer() else float(val),
                    "type": "integer" if float(val).is_integer() else "float"
                })
            except (ValueError, OverflowError):
                continue

        else:
            str_val = str(val).strip()
            if str_val:
                tags.append({"key": clean_key, "value": str_val, "type": "string"})

    transcription     = row.get("Transcription")
    parsed_transcript = (
        parse_transcript(str(transcription))
        if pd.notna(transcription) and str(transcription).strip()
        else None
    )

    payload = {
        "filename":    f"recording_{row['masterContactId']}.mp4",
        "agent_email": row["Agent_Email"],
        "product_id":  product_id,
        "speaker":     {"separation": "channels", "channel_map": ["agent", "customer"]},
        "transcript":  parsed_transcript,
        "tags":        tags,
    }

    response = requests.post("https://app.reddy.io/api/v1/call/create", headers=headers, json=payload)

    if response.status_code == 200:
        call_info  = response.json()
        upload_url = call_info.get("upload_url") or call_info.get("signed_url") or call_info.get("signed_token")
        logging.info(f"✓ Reddy call created for masterContactId={row['masterContactId']}")
        return {
            "call_id":         call_info.get("call_id"),
            "conversation_id": call_info.get("conversation_id"),
            "upload_url":      upload_url,
            "filename":        payload["filename"],
        }

    logging.error(f"Reddy create failed for {row['masterContactId']}: {response.status_code} — {response.text}")
    return {"error": response.text, "status_code": response.status_code}


def upload_audio_to_reddy(upload_url, audio_bytes, filename):
    audio_bytes.seek(0)
    return requests.put(
        upload_url,
        data=audio_bytes.read(),
        headers={"Content-Type": "audio/mp4"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — process_call
# ─────────────────────────────────────────────────────────────────────────────

def process_call(row: pd.Series, product_id: int) -> dict:
    # ── Guard: required fields must be present, else skip cleanly ─────────
    agent_email = row.get("Agent_Email")
    if pd.isna(agent_email) or not str(agent_email).strip():
        logging.warning(
            f"Skipping ContactId={row.get('ContactId')} — no Agent_Email "
            f"(Owner__c={row.get('OwnerId')} not in consultant table)"
        )
        return {"success": False, "error": "missing_agent_email",
                "owner_id": str(row.get("OwnerId")) if pd.notna(row.get("OwnerId")) else None}

    master_id  = row.get("masterContactId")
    contact_id = row.get("ContactId")
    if (pd.isna(master_id) or master_id in ("", None)) and (pd.isna(contact_id) or contact_id in ("", None)):
        logging.warning(f"Skipping row — no masterContactId and no ContactId")
        return {"success": False, "error": "missing_contact_id"}

    segment_id = row.get("SegmentId")
    logging.info(f"=== Processing ContactId={contact_id} | masterContactId={master_id} | SegmentId={segment_id} ===")

    if pd.isna(segment_id) or not segment_id:
        logging.warning(f"No SegmentId for ContactId={contact_id} — trying contact fallback")
        audio_bytes = download_contact_recording(str(int(float(master_id))))
    else:
        audio_bytes = download_segment_recording(segment_id)
        if audio_bytes is None:
            logging.warning(f"Segment download failed for SegmentId={segment_id} — trying contact fallback")
            audio_bytes = download_contact_recording(str(int(float(master_id))))

    if audio_bytes is None:
        logging.error(f"Both segment and contact download failed for ContactId={contact_id}")
        return {"success": False, "error": "download_failed"}

    reddy_info = upload_call_to_reddy(row, product_id)
    if "error" in reddy_info:
        logging.error(f"Reddy create failed for ContactId={contact_id}")
        return {"success": False, "error": "reddy_create_failed", "details": reddy_info}

    upload_response = upload_audio_to_reddy(reddy_info["upload_url"], audio_bytes, reddy_info["filename"])
    if upload_response.status_code not in (200, 201):
        logging.error(f"Audio upload failed for SegmentId={segment_id}: {upload_response.text}")
        return {"success": False, "error": "upload_failed",
                "status_code": upload_response.status_code, "response": upload_response.text}

    logging.info(f"✓ Successfully processed ContactId={contact_id}")
    return {"success": True, "call_id": reddy_info["call_id"], "filename": reddy_info["filename"]}


# ─────────────────────────────────────────────────────────────────────────────
# Reddy_Upload  —  call attributed by calldata.Owner__c (who was on the call)
# ─────────────────────────────────────────────────────────────────────────────
@app.function_name(name="Reddy_Upload")
@app.route(route="Reddy_Upload")
def test_users(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Reddy_Upload triggered")

    # If True, only upload calls that also link to an intake in intake_reddy.csv
    # (so the Reddy tag payload is populated). If False, owner-matched calls with
    # no intake link are still uploaded, but with null intake-derived tags.
    REQUIRE_INTAKE_LINK = True

    try:
        # ── STEP 1A: Load Salesforce data ────────────────────────────────────
        case_consultants = load_case_consultants_for_director(blob_service_client, DIRECTOR_EMAILS)
        consultant_names = set(case_consultants["Intake_Agent_Name"].dropna().unique())
        logging.info(f"Consultant universe size: {len(consultant_names)}")

        intakes  = get_sf_intakes()
        calldata = load_salesforce_calldata(blob_service_client)
        logging.info(f"consultant_names sample: {list(consultant_names)[:5]}")
        logging.info(f"intake OwnerId sample:      {intakes['OwnerId'].dropna().unique()[:5].tolist()}")
        logging.info(f"intake CreatedById sample:  {intakes['CreatedById'].dropna().unique()[:5].tolist()}")
        logging.info(f"intake Intake_Agent_Name sample: {intakes['Intake_Agent_Name'].dropna().unique()[:5].tolist()}")
        # Intakes assigned to consultants, by case type (diagnostic)
        assigned = intakes[intakes["CreatedById"].isin(consultant_names)]
        logging.info(f"Intakes assigned to consultants: {len(assigned)}")
        for case_type, count in assigned["Case_Type__c"].value_counts(dropna=False).items():
            logging.info(f"    {case_type}: {count}")

        # ── STEP 1B: ATTRIBUTE CALLS BY WHO WAS ON THE CALL (Owner__c) ────────
        # load_salesforce_calldata renamed Owner__c -> OwnerId.
        calldata["OwnerId"] = calldata["OwnerId"].astype(str).str.strip()
        consultant_calls = calldata[calldata["OwnerId"].isin(consultant_names)].copy()
        logging.info(f"Calls attributed to consultants by Owner__c: {len(consultant_calls)}")
        
        if consultant_calls.empty:
            logging.warning("No calls matched any consultant via Owner__c")
            return func.HttpResponse(
                json.dumps({"success": 0, "failed": 0, "no_segment": 0,
                            "message": "No matching calls found"}),
                status_code=200, mimetype="application/json",
            )

        # ── STEP 1C: ENRICH from intake via IntakeId (LEFT join, not a filter) ─
        # The intake link only adds context (case type, SOL, tags). It does NOT
        # decide attribution — that was already done above by Owner__c.
        consultant_calls = consultant_calls.merge(
            intakes,
            on="IntakeId",
            how="left",
            suffixes=("_call", "_intake"),
        )

        has_intake = consultant_calls["Case_Type__c"].notna()
        logging.info(
            f"Owner-matched calls WITH intake context: {has_intake.sum()} | "
            f"WITHOUT (tags will be sparse): {(~has_intake).sum()}"
        )

        if REQUIRE_INTAKE_LINK:
            consultant_calls = consultant_calls[has_intake].reset_index(drop=True)
            logging.info(f"REQUIRE_INTAKE_LINK=True → kept {len(consultant_calls)} calls")

        # Resolve columns duplicated by the intake merge. Prefer the CALL's own
        # values — attribution is by who was on the call, so *_call wins.
        for col in ["CreatedDate", "IsDeleted", "LastModifiedDate", "SystemModstamp", "OwnerId"]:
            call_col, intake_col = f"{col}_call", f"{col}_intake"
            if call_col in consultant_calls.columns and intake_col in consultant_calls.columns:
                consultant_calls[col] = consultant_calls[call_col]
                consultant_calls = consultant_calls.drop(columns=[call_col, intake_col])
            elif call_col in consultant_calls.columns:
                consultant_calls = consultant_calls.rename(columns={call_col: col})
            elif intake_col in consultant_calls.columns:
                consultant_calls = consultant_calls.rename(columns={intake_col: col})

        # ── masterContactId fallback to ContactId ────────────────────────────
        consultant_calls["masterContactId"] = consultant_calls["masterContactId"].fillna(
            pd.to_numeric(consultant_calls["ContactId"], errors="coerce").astype("Int64")
        )
        consultant_calls = consultant_calls[
            consultant_calls["masterContactId"].notna() | consultant_calls["ContactId"].notna()
        ].reset_index(drop=True)
        logging.info(f"Rows after contact-id fallback: {len(consultant_calls)}")

        # ── Agent email comes from WHO WAS ON THE CALL (Owner__c == OwnerId) ──
        # Join the consultant table on its canonical agent id to get the email.
        consultant_calls = consultant_calls.merge(
            case_consultants[["Intake_Agent_Name", "Email"]],
            left_on="OwnerId",
            right_on="Intake_Agent_Name",
            how="left",
        ).rename(columns={"Email": "Agent_Email"})

        logging.info(
            f"Rows with Agent_Email populated: "
            f"{consultant_calls['Agent_Email'].notna().sum()} / {len(consultant_calls)}"
        )

        # ── Column ordering ──────────────────────────────────────────────────
        priority_cols = [
            "ContactId", "masterContactId", "SegmentId", "CreatedDate",
            "Agent_Email", "OwnerId", "IntakeId", "SkillName",
            "DispositionName", "DispositionNotes",
        ]
        other_cols = [c for c in consultant_calls.columns if c not in priority_cols]
        consultant_calls = consultant_calls.reindex(columns=priority_cols + other_cols)

        # ── STEP 1D: Save pre-enrichment file ────────────────────────────────
        storage_container = blob_service_client.get_container_client("ccc-data")
        save_csv(storage_container, consultant_calls, "Reddy/testfile.csv")

        # ── STEP 2: Data Extraction job for the date range ───────────────────
        valid_dates = pd.to_datetime(consultant_calls["CreatedDate"], errors="coerce").dropna()
        if valid_dates.empty:
            valid_dates = pd.to_datetime(intakes["CreatedDate"], errors="coerce").dropna()
        start_date = valid_dates.min().strftime("%Y-%m-%d")
        end_date   = valid_dates.max().strftime("%Y-%m-%d")
        logging.info(f"Data Extraction date range: {start_date} → {end_date}")
        extraction_df = run_extraction_job(start_date, end_date)

        # ── STEP 3: ContactId → SegmentId ────────────────────────────────────
        contact_ids    = pd.to_numeric(consultant_calls["ContactId"], errors="coerce")
        segment_lookup = build_segment_lookup(extraction_df, contact_ids)
        consultant_calls["SegmentId"] = (
            pd.to_numeric(consultant_calls["ContactId"], errors="coerce")
            .astype("Int64").map(segment_lookup)
        )
        logging.info(
            f"SegmentId matched: {consultant_calls['SegmentId'].notna().sum()} | "
            f"unmatched: {consultant_calls['SegmentId'].isna().sum()}"
        )
        save_csv(storage_container, consultant_calls, "Reddy/testfile.csv")

        # ── STEP 4: Filter dispositions + SS cap ─────────────────────────────
        consultant_calls = consultant_calls[
            (~consultant_calls["DispositionName"].isin(EXCLUDE_DISPOSITIONS))
            & (consultant_calls["DispositionName"].notna())
        ]

        SS_UPLOAD_LIMIT = 50
        # Case_Type__c may be null for owner-matched calls with no intake link;
        # those are treated as non-SS and are never capped.
        is_ss  = consultant_calls["Case_Type__c"] == "Social Security"
        non_ss = consultant_calls[~is_ss]
        ss     = consultant_calls[is_ss].head(SS_UPLOAD_LIMIT)
        consultant_calls = pd.concat([non_ss, ss]).reset_index(drop=True)
        logging.info(f"After SS cap: {len(consultant_calls)} ({len(ss)} SS, {len(non_ss)} other)")

        # ── STEP 5: Process ──────────────────────────────────────────────────
        logging.info(f"Processing {len(consultant_calls)} calls")
        results = {"success": 0, "failed": 0, "no_segment": 0, "errors": []}
        for _, row in consultant_calls.iterrows():
            result = process_call(row, REDDY_PRODUCT_ID)
            if result.get("error") == "no_segment_id":
                results["no_segment"] += 1
            elif result.get("success"):
                results["success"] += 1
            else:
                results["failed"] += 1
                results["errors"].append({
                    "contact_id": str(row["ContactId"]) if pd.notna(row["ContactId"]) else None,
                    "segment_id": str(row.get("SegmentId")) if pd.notna(row.get("SegmentId")) else None,
                    "error":   result.get("error"),
                    "details": result.get("details"),
                })

        logging.info(f"Final Results: {results}")
        return func.HttpResponse(
            json.dumps(sanitize_for_json(results), indent=2),
            status_code=200, mimetype="application/json",
        )

    except Exception as e:
        logging.error(f"Error in test_users: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)
    

# ─────────────────────────────────────────────────────────────────────────────
# Azure Function — List_Consultants
# ─────────────────────────────────────────────────────────────────────────────

@app.function_name(name="List_Consultants")
@app.route(route="list-consultants")
def list_consultants(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("List_Consultants triggered")
    try:
        case_consultants = load_case_consultants_for_director(blob_service_client, DIRECTOR_EMAILS)
        output = case_consultants[["Email", "FirstName", "LastName", "Title", "Intake_Agent_Name"]].to_dict(orient="records")
        return func.HttpResponse(
            json.dumps(sanitize_for_json(output), indent=2),
            status_code=200,
            mimetype="application/json",
        )
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)
