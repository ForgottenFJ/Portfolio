import azure.functions as func
import json, logging, os, re
from datetime import datetime, timedelta, timezone
from io import BytesIO
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.keyvault.secrets import SecretClient
from azure.identity import EnvironmentCredential, ClientSecretCredential
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, TextStringObject, BooleanObject, NumberObject, DictionaryObject
from dataclasses import dataclass, field
from typing import Callable, Optional

# ============================================================
# Configuration
# ============================================================
tenant_id = os.environ["azure_tenant_id"]
client_id = os.environ["azure_client_id"]
client_secret = os.environ["azure_client_secret"]
azure_vault_url = os.environ["KEY_VAULT_URL"]
Devmode = os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT") == "Development"

if Devmode:
    logging.info("Using EnvironmentCredential for local development.")
    credential = EnvironmentCredential()
else:
    logging.info("Using ClientSecretCredential for Azure environment.")
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)

client = SecretClient(vault_url=azure_vault_url, credential=credential)

app = func.FunctionApp()

# ============================================================
# Helper Functions
# ============================================================
def sanitize_filename(name: str) -> str:
    """Remove potentially dangerous characters from filename components."""
    return re.sub(r'[^\w\-.]', '_', str(name))


# ============================================================
# State Configuration
# ============================================================
@dataclass
class StateConfig:
    """Configuration for a specific form state."""
    output_folder: str
    filename_template: str = "{unique_id}_{first_name}_{last_name}.pdf"
    flatten_pdf: bool = False
    custom_field_processors: dict = field(default_factory=dict)
    pre_process_hook: Optional[Callable[[dict], dict]] = None
    
    def build_filename(self, form_data: dict) -> str:
        """Build filename from template and form data."""
        unique_id = sanitize_filename(form_data.get("UniqueID", "unknown_id"))
        first_name = sanitize_filename(form_data.get("name", {}).get("first", "unknown"))
        last_name = sanitize_filename(form_data.get("name", {}).get("last", "unknown"))
        date_of_injury = form_data.get("date_of_injury", "")
        claim_number = sanitize_filename(form_data.get("claim_number", ""))
        
        return self.filename_template.format(
            unique_id=unique_id,
            first_name=first_name,
            last_name=last_name,
            date_of_injury=date_of_injury.replace("/", "-"),
            claim_number=claim_number,
        )


STATE_CONFIGS: dict[str, StateConfig] = {
    "new_york": StateConfig(
        output_folder="CCC/Formstack/Workcomp/New_York",
        filename_template="{unique_id}_{first_name}_{last_name}.pdf",
        flatten_pdf=False,
    ),
    "test": StateConfig(
        output_folder="CCC/Formstack/Workcomp/Test",
        filename_template="TEST_{unique_id}_{first_name}_{last_name}.pdf",
        flatten_pdf=False,
    ),
}


# ============================================================
# Response Helpers
# ============================================================
def error_response(message: str, status_code: int) -> func.HttpResponse:
    """Helper to create consistent error responses."""
    return func.HttpResponse(
        json.dumps({"status": "error", "message": message}),
        status_code=status_code,
        mimetype="application/json"
    )


def success_response(file_url: str) -> func.HttpResponse:
    """Helper to create consistent success responses."""
    return func.HttpResponse(
        json.dumps({
            "status": "success",
            "file_url": file_url,
            "message": "PDF form filled and uploaded successfully"
        }),
        status_code=200,
        mimetype="application/json"
    )


# ============================================================
# Data Processing Helpers
# ============================================================
def process_time_of_injury(form_data: dict) -> None:
    """
    Parse time_of_injury field and add derived fields.
    Modifies form_data in place.
    """
    time_of_injury = form_data.get("time_of_injury", "").strip()
    if not time_of_injury:
        return
    
    try:
        parsed_time = datetime.strptime(time_of_injury, "%I:%M %p")
        form_data["time_of_injury_updated"] = parsed_time.strftime("%I:%M")
        
        injury_period = parsed_time.strftime("%p")
        if injury_period in ("AM", "PM"):
            form_data["time_am_pm"] = injury_period
        else:
            logging.warning(f"Unexpected time period format: {injury_period}")
    except ValueError:
        logging.error(f"Invalid time format: {time_of_injury}")

def process_date_fields(form_data: dict, date_key: str, field_prefix: str) -> None:
    """
    Parse a date field and add split month, day, year components.
    Keeps the original date value intact for fields that need the full date.
    Modifies form_data in place.
    
    Args:
        form_data: The form data dictionary
        date_key: The key containing the date (e.g., "date_of_injury_or_onset_of_illness")
        field_prefix: Prefix for the new keys (e.g., "date_of_injury")
    """
    date_value = form_data.get(date_key, "")
    logging.info(f"Processing date field '{date_key}': raw value = '{date_value}', type = {type(date_value)}")
    if not date_value or not isinstance(date_value, str):
        return
    
    date_value = date_value.strip()
    
    try:
        # Parse "Jan 14, 2026" format
        parsed_date = datetime.strptime(date_value, "%b %d, %Y")
        form_data[f"{field_prefix}_month"] = parsed_date.strftime("%m")  # "01"
        form_data[f"{field_prefix}_day"] = parsed_date.strftime("%d")    # "14"
        form_data[f"{field_prefix}_year"] = parsed_date.strftime("%Y")   # "2026"
        logging.info(f"Parsed date {date_key}: month={parsed_date.strftime('%m')}, day={parsed_date.strftime('%d')}, year={parsed_date.strftime('%Y')}")
    except ValueError:
        # Try alternative format "01/14/2026"
        try:
            parsed_date = datetime.strptime(date_value, "%m/%d/%Y")
            form_data[f"{field_prefix}_month"] = parsed_date.strftime("%m")
            form_data[f"{field_prefix}_day"] = parsed_date.strftime("%d")
            form_data[f"{field_prefix}_year"] = parsed_date.strftime("%Y")
            logging.info(f"Parsed date {date_key} (alt format): month={parsed_date.strftime('%m')}, day={parsed_date.strftime('%d')}, year={parsed_date.strftime('%Y')}")
        except ValueError:
            logging.error(f"Invalid date format for {date_key}: {date_value}")

def process_phone_fields(form_data: dict, phone_key: str, field_prefix: str) -> None:
    """
    Parse a phone number field and split into area code and number components.
    Keeps the original phone value intact.
    Modifies form_data in place.
    
    Args:
        form_data: The form data dictionary
        phone_key: The key containing the phone (e.g., "phone_number")
        field_prefix: Prefix for the new keys (e.g., "phone")
    """
    phone_value = form_data.get(phone_key, "")
    if not phone_value or not isinstance(phone_value, str):
        return
    
    # Remove all non-numeric characters
    digits = re.sub(r'\D', '', phone_value)
    
    if len(digits) == 10:
        form_data[f"{field_prefix}_area_code"] = digits[:3]      # "305"
        form_data[f"{field_prefix}_number"] = digits[3:]          # "5551234"
        logging.info(f"Parsed phone {phone_key}: area_code={digits[:3]}, number={digits[3:]}")
    elif len(digits) == 11 and digits.startswith('1'):
        # Handle numbers with leading 1 (e.g., 1-305-555-1234)
        form_data[f"{field_prefix}_area_code"] = digits[1:4]      # "305"
        form_data[f"{field_prefix}_number"] = digits[4:]          # "5551234"
        logging.info(f"Parsed phone {phone_key}: area_code={digits[1:4]}, number={digits[4:]}")
    else:
        logging.warning(f"Unexpected phone format for {phone_key}: {phone_value}")

def combine_composite_fields(form_data: dict) -> None:
    """Combine address and name components into single fields."""
    
    # Address fields
    address_fields = [
        ("mailing_address", "mailing_address_full"),
        ("employer_address", "employer_address_full"),
    ]
    
    for source_key, target_key in address_fields:
        address_data = form_data.get(source_key, {})
        if isinstance(address_data, dict):
            address = address_data.get("address", "")
            city = address_data.get("city", "")
            state = address_data.get("state", "")
            zip_code = address_data.get("zip", "")
            
            # Format: "850 3rd Avenue, Brooklyn, NY 10010"
            full_address = f"{address}, {city}, {state} {zip_code}".strip()
            form_data[target_key] = full_address
            logging.info(f"Combined address: {target_key} = '{full_address}'")
    
    # Name field
    name_data = form_data.get("name", {})
    if isinstance(name_data, dict):
        first = name_data.get("first", "")
        middle = name_data.get("middle", "")
        last = name_data.get("last", "")
        
        # Format: "Drew F Dannenbaum" or "Drew Dannenbaum" if no middle
        if middle:
            full_name = f"{first} {middle} {last}".strip()
        else:
            full_name = f"{first} {last}".strip()
        
        form_data["full_name"] = full_name
        logging.info(f"Combined name: full_name = '{full_name}'")

def process_doctor_fields(form_data: dict, doctor_key: str, field_prefix: str) -> None:
    raw_value = form_data.get(doctor_key, "")
    if not raw_value or not isinstance(raw_value, str):
        return

    # Look for a street number (digits) followed by a word, indicating address start
    address_pattern = re.compile(r'\b(\d+\s+[A-Za-z]+)')
    
    match = address_pattern.search(raw_value)
    if match:
        split_pos = match.start()
        name = raw_value[:split_pos].strip().rstrip(',')
        full_address = raw_value[split_pos:].strip()
    else:
        # No address pattern found - check for comma fallback
        parts = [p.strip() for p in raw_value.split(",", 1)]
        name = parts[0]
        full_address = parts[1] if len(parts) == 2 else ""

    form_data[f"{field_prefix}_name"] = name
    form_data[f"{field_prefix}_address"] = full_address

    logging.info(
        f"Parsed doctor field '{doctor_key}': "
        f"name={name}, full_address={full_address}"
    )
    
# ============================================================
# Azure Function Entry Point
# ============================================================
@app.function_name(name="FillPDFForm")
@app.route(route="fillpdf", methods=["POST"])
def fill_pdf_form(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('PDF form filling function triggered.')
    
    try:
        req_body = req.get_json()
        formstack_data = req_body.get("formstack_data", {})
        form_state = req_body.get("form_state") or formstack_data.get("form_state")

        if not form_state:
            return error_response("Missing form_state in request", 400)
        
        if form_state not in STATE_CONFIGS:
            return error_response(
                f"Unsupported form_state: {form_state}. Supported: {list(STATE_CONFIGS.keys())}", 
                400
            )
        
        config = STATE_CONFIGS[form_state]
        file_url = process_pdf_form(req_body, formstack_data, config)
        
        return success_response(file_url)
            
    except Exception as e:
        logging.error(f"Error processing PDF: {str(e)}")
        return error_response(str(e), 500)


# ============================================================
# Core Processing
# ============================================================
def process_pdf_form(req_body: dict, form_data: dict, config: StateConfig) -> str:
    """
    Process PDF form filling using state-specific configuration.
    
    Args:
        req_body: Full request body
        form_data: The formstack_data dictionary
        config: State-specific configuration
    
    Returns:
        URL to the uploaded filled PDF
    """
    # Build filename using state-specific template
    filename = config.build_filename(form_data)
    logging.info(f"Generated filename: {filename}")
    
    combine_composite_fields(form_data)

    # Process time of injury if present
    process_time_of_injury(form_data)
    process_date_fields(form_data, "todays_date", "todays_date")
    process_date_fields(form_data, "date_of_birth", "date_of_birth")
    process_date_fields(form_data, "date_of_hire", "date_of_hire")
    process_date_fields(form_data, "date_of_injury_or_onset_of_illness", "date_of_injury")
    process_date_fields(form_data, "what_date_was_the_notice_given", "date_of_notice")
    process_date_fields(form_data, "on_what_date_did_you_stop_work", "stop_work_date")
    process_date_fields(form_data, "on_what_date_did_you_return_to_work", "return_work_date")
    process_date_fields(form_data, "what_was_the_date_of_your_first_treatment", "first_treatment_date")

    process_phone_fields(form_data, "phone_number", "phone")
    process_phone_fields(form_data, "employer_phone_number", "emp_phone")
    process_phone_fields(form_data, "phone_number_of_where_you_first_treated", "first_treated_phone")
    process_phone_fields(form_data, "phone_number_of_the_doctors_currently_treating_you_for_this_injury__illness", "doctor_phone")

    process_doctor_fields(form_data, "name_and_address_where_you_first_treated", "first_treatment")
    process_doctor_fields(form_data, "give_the_name_and_address_of_the_doctors_treating_you_for_this_injury__illness", "treating_doctor")
    
    # Apply pre-processing hook if defined
    if config.pre_process_hook:
        form_data = config.pre_process_hook(form_data)
    
    # Download PDF template
    template_blob_name = req_body.get('pdf_template_url')
    if not template_blob_name:
        raise ValueError("Missing pdf_template_url in request")
    
    pdf_bytes = download_pdf_template(template_blob_name)
    
    # Load mappings from the same directory as the template
    mapping_dir = "/".join(template_blob_name.split("/")[:-1])
    
    try:
        field_mapping = load_json_file(f"{mapping_dir}/field_mapping.json")
    except Exception as e:
        logging.error(f"Error loading field_mapping.json: {str(e)}")
        raise
    
    try:
        checkbox_mapping = load_json_file(f"{mapping_dir}/checkbox_mapping.json")
    except Exception as e:
        logging.error(f"Error loading checkbox_mapping.json: {str(e)}")
        raise
    
    # Get PDF fields and map form data
    pdf_fields = get_pdf_field_names(pdf_bytes)
    #logging.info(f"All PDF fields: {pdf_fields}")
    phone_fields = [f for f in pdf_fields if 'phone' in f.lower()]
    logging.info(f"Phone fields in PDF: {phone_fields}")
    
    date_fields = [f for f in pdf_fields if 'date' in f.lower()]
    logging.info(f"Date fields in PDF: {date_fields}")
    
    birth_fields = [f for f in pdf_fields if 'birth' in f.lower()]
    logging.info(f"Birth fields in PDF: {birth_fields}")

    mapped_form_data = map_form_data(form_data, field_mapping, checkbox_mapping)
    
    # Check for missing fields
    missing_fields = [f for f in mapped_form_data if f not in pdf_fields]
    if missing_fields:
        logging.warning(f"Form data contains fields not in PDF: {missing_fields}")
    
    # Fill PDF form
    filled_pdf_bytes = fill_pdf_fields(pdf_bytes, mapped_form_data)
    
    # Flatten if required by state config
    if config.flatten_pdf:
        filled_pdf_bytes = flatten_pdf_pypdf(filled_pdf_bytes)
        logging.info("PDF flattened per state configuration")
    
    # Upload to storage using config's output folder
    return upload_to_storage(filled_pdf_bytes, filename, config.output_folder)


# ============================================================
# Storage Functions
# ============================================================
def download_pdf_template(template_blob_name: str) -> bytes:
    """Download PDF template from Azure Blob Storage using secure credentials."""
    try:
        connection_string = client.get_secret("azure-connection-string").value
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(
            container=os.environ["TEMPLATE_CONTAINER"],
            blob=template_blob_name
        )
        pdf_bytes = blob_client.download_blob().readall()
        logging.info(f"Downloaded PDF template: {template_blob_name}")
        return pdf_bytes
    except Exception as e:
        logging.error(f"Error downloading PDF template: {str(e)}")
        raise


def load_json_file(template_config_name: str) -> dict:
    """Load JSON configuration file from Azure Blob Storage."""
    try:
        connection_string = client.get_secret("azure-connection-string").value
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(
            container=os.environ["TEMPLATE_CONTAINER"],
            blob=template_config_name
        )
        json_bytes = blob_client.download_blob().readall()
        json_data = json.loads(json_bytes.decode('utf-8'))
        logging.info(f"Downloaded JSON file: {template_config_name}")
        return json_data
    except Exception as e:
        logging.error(f"Error downloading JSON file: {str(e)}")
        raise


def upload_to_storage(pdf_bytes: bytes, filename: str, output_folder: str) -> str:
    """
    Upload filled PDF to Azure Blob Storage.
    
    Args:
        pdf_bytes: The filled PDF content
        filename: The filename for the PDF
        output_folder: The folder path from state config
    
    Returns:
        SAS URL for accessing the uploaded file
    """
    try:
        connection_string = client.get_secret("azure-connection-string").value
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = os.environ["OUTPUT_CONTAINER"]
        
        # Use the output_folder from config instead of hardcoded path
        blob_name = f"{output_folder}/{filename}"
        
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )

        blob_client.upload_blob(pdf_bytes, overwrite=True)
        logging.info(f"Uploaded PDF to: {blob_name}")

        sas_token = generate_blob_sas(
            account_name=os.environ["STORAGE_ACCOUNT_NAME"],
            container_name=container_name,
            blob_name=blob_name,
            account_key=client.get_secret("azure-storage-key").value,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1)
        )

        return f"https://{os.environ['STORAGE_ACCOUNT_NAME']}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        
    except Exception as e:
        logging.error(f"Error uploading PDF: {str(e)}")
        raise


# ============================================================
# PDF Processing Functions
# ============================================================
def map_form_data(form_data: dict, field_mapping: dict, checkbox_mapping: dict = None) -> dict:
    """Map form data to PDF field names, handling nested objects and arrays."""
    mapped = {}
    
    for key, value in form_data.items():
        # Special handling for time_of_injury to extract AM/PM
        if key == "time_of_injury" and isinstance(value, str):
            time_parts = value.split()
            if len(time_parts) >= 2:
                time_value = time_parts[0]
                am_pm = time_parts[1].upper()
                
                if key in field_mapping:
                    mapped[field_mapping[key]] = time_value
                
                if checkbox_mapping and "time_am_pm" in checkbox_mapping:
                    checkbox_config = checkbox_mapping["time_am_pm"]
                    if isinstance(checkbox_config, dict) and am_pm in checkbox_config:
                        pdf_field = checkbox_config[am_pm]
                        mapped[pdf_field] = True
                        logging.info(f"Time AM/PM: {am_pm} -> {pdf_field}")
            continue
        
        # Handle nested objects (like addresses)
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                full_key = f"{key}.{sub_key}"
                if full_key in field_mapping:
                    mapped[field_mapping[full_key]] = sub_value
                elif sub_key in field_mapping:
                    mapped[field_mapping[sub_key]] = sub_value
                elif checkbox_mapping:
                    if full_key in checkbox_mapping:
                        checkbox_config = checkbox_mapping[full_key]
                        if isinstance(checkbox_config, dict) and sub_value in checkbox_config:
                            mapped[checkbox_config[sub_value]] = True
                        elif isinstance(checkbox_config, str):
                            mapped[checkbox_config] = normalize_checkbox(sub_value)
                    elif sub_key in checkbox_mapping:
                        checkbox_config = checkbox_mapping[sub_key]
                        if isinstance(checkbox_config, dict) and sub_value in checkbox_config:
                            mapped[checkbox_config[sub_value]] = True
                        elif isinstance(checkbox_config, str):
                            mapped[checkbox_config] = normalize_checkbox(sub_value)
            continue
        
        # Handle arrays (like checkbox: ["AM"] or ["Yes_2"])
        if isinstance(value, list):
            first_val = value[0] if len(value) > 0 else None
            if key in field_mapping:
                pdf_field = field_mapping[key]
                if isinstance(pdf_field, str):
                    mapped[pdf_field] = first_val if first_val is not None else False
            elif checkbox_mapping and key in checkbox_mapping:
                checkbox_config = checkbox_mapping[key]
                if isinstance(checkbox_config, dict) and first_val in checkbox_config:
                    pdf_field = checkbox_config[first_val]
                    mapped[pdf_field] = True
                    logging.info(f"Checkbox mapping: {key}[{first_val}] -> {pdf_field}")
                elif isinstance(checkbox_config, str):
                    mapped[checkbox_config] = first_val if first_val is not None else False
            continue
        
        # Regular field mapping
        if key in field_mapping:
            pdf_field = field_mapping[key]
            if isinstance(pdf_field, str):
                mapped[pdf_field] = value
            else:
                logging.warning(f"Field mapping for '{key}' is not a string: {pdf_field}")
        elif checkbox_mapping and key in checkbox_mapping:
            checkbox_config = checkbox_mapping[key]
            if isinstance(checkbox_config, dict):
                # Handle "Other: <text>" pattern FIRST - check before exact match
                if isinstance(value, str) and (value.startswith("Other:") or value.startswith("Other ")):
                    logging.info(f"Detected 'Other' pattern for {key}: '{value}'")
                    
                    # Check the "Other" checkbox
                    if "Other" in checkbox_config:
                        pdf_field = checkbox_config["Other"]
                        mapped[pdf_field] = True
                        logging.info(f"Checkbox mapping: {key}=Other -> {pdf_field} = True")
                    else:
                        logging.warning(f"No 'Other' key in checkbox_config for '{key}'")
                    
                    # Extract and map the text value - handle both "Other:" and "Other " patterns
                    if ":" in value:
                        other_text = value.split(":", 1)[1].strip()
                    else:
                        other_text = value.replace("Other", "", 1).strip()
                    
                    if other_text and "Other_Text" in checkbox_config:
                        text_field = checkbox_config["Other_Text"]
                        mapped[text_field] = other_text
                        logging.info(f"Other text mapping: {key} -> {text_field} = '{other_text}'")
                    elif other_text:
                        logging.warning(f"No 'Other_Text' key in checkbox_config for '{key}', text was: '{other_text}'")
                
                elif value in checkbox_config:
                    pdf_field = checkbox_config[value]
                    mapped[pdf_field] = True
                    logging.info(f"Checkbox mapping: {key}={value} -> {pdf_field} = True")
                else:
                    # Only warn if it's not an "Other" pattern we already handled
                    logging.warning(f"Value '{value}' not found in checkbox mapping for '{key}'. Available: {list(checkbox_config.keys())}")
            elif isinstance(checkbox_config, str):
                mapped[checkbox_config] = normalize_checkbox(value)
    
    return mapped


def normalize_checkbox(value) -> bool:
    """Normalize checkbox values to boolean or string."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in {"on", "yes", "true", "1"}:
            return True
        elif lower in {"off", "no", "false", "0"}:
            return False
        else:
            return value
    return False


def fill_pdf_fields(pdf_bytes: bytes, form_data: dict) -> bytes:
    """Fill AcroForm fields in a PDF, including text fields, checkboxes, and radio buttons."""
    logging.info("=== Starting PDF fill process ===")
    logging.info(f"Received form_data keys: {list(form_data.keys())}")
    
    # Flatten the form_data to handle nested objects and arrays
    flattened_data = {}
    for key, value in form_data.items():
        try:
            if isinstance(key, dict):
                logging.error(f"Key is a dict! {key}")
                continue
            key_str = str(key)
            
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if isinstance(sub_key, dict):
                        logging.error(f"Sub-key is a dict! {sub_key}")
                        continue
                    sub_key_str = str(sub_key)
                    if not isinstance(sub_value, (dict, list)):
                        flattened_data[f"{key_str}.{sub_key_str}"] = sub_value
                        flattened_data[sub_key_str] = sub_value
            elif isinstance(value, list):
                if len(value) > 0:
                    first_val = value[0]
                    if isinstance(first_val, dict):
                        logging.warning(f"Array contains dict for '{key_str}', skipping")
                        continue
                    flattened_data[key_str] = first_val
                else:
                    flattened_data[key_str] = False
            else:
                if not isinstance(value, dict):
                    flattened_data[key_str] = value
        except Exception as e:
            logging.error(f"Error processing key '{key}': {e}")
            continue
    
    logging.info(f"Flattened to {len(flattened_data)} fields")
    
    reader = PdfReader(BytesIO(pdf_bytes))
    writer = PdfWriter()
    writer.append(reader)
    
    # Ensure AcroForm exists
    if "/AcroForm" not in writer._root_object:
        writer._root_object[NameObject("/AcroForm")] = DictionaryObject()
    
    acro_form = writer._root_object["/AcroForm"]
    if hasattr(acro_form, "get_object"):
        acro_form = acro_form.get_object()
    
    # Utility helpers
    def _as_name(value):
        if isinstance(value, NameObject):
            return value
        s = str(value)
        if not s.startswith("/"):
            s = "/" + s
        return NameObject(s)
    
    def _get_widgets(field_dict):
        kids = field_dict.get("/Kids")
        if kids:
            out = []
            for kid in kids:
                try:
                    out.append(kid.get_object() if hasattr(kid, "get_object") else kid)
                except Exception:
                    continue
            return out
        return [field_dict]
    
    def _on_state_name(widget):
        try:
            ap = widget.get("/AP")
            if ap and isinstance(ap, DictionaryObject):
                n = ap.get("/N")
                if isinstance(n, DictionaryObject):
                    for key in n.keys():
                        if key != NameObject("/Off"):
                            return key
        except Exception:
            pass
        try:
            asv = widget.get("/AS")
            if asv and asv != NameObject("/Off"):
                return asv
        except Exception:
            pass
        return NameObject("/Yes")
    
    # Process fields
    fields = acro_form.get("/Fields", [])
    logging.info(f"Found {len(fields)} fields in PDF AcroForm")
    
    for field_ref in fields:
        try:
            field = field_ref.get_object() if hasattr(field_ref, "get_object") else field_ref
        except Exception as e:
            logging.warning(f"Could not dereference field: {e}")
            continue
        
        try:
            field_name_obj = field.get("/T")
            if not field_name_obj:
                continue
            field_name = str(field_name_obj)
            if isinstance(field_name, dict):
                logging.error(f"Field name is a dict: {field_name}")
                continue
        except Exception as e:
            logging.warning(f"Could not get field name: {e}")
            continue
        
        if field_name not in flattened_data:
            continue
        
        raw_value = flattened_data[field_name]
        ft = field.get("/FT")
        widgets = _get_widgets(field)
        
        # Button fields: Checkboxes and Radio
        if ft == NameObject("/Btn"):
            logging.info(f"Button field: '{field_name}' = {raw_value}")
            
            is_bool = isinstance(raw_value, bool)
            is_off_string = isinstance(raw_value, str) and raw_value.strip().lower() in {"off", "/off", "false", "0", "no"}
            
            if is_bool:
                for w in widgets:
                    on_name = _on_state_name(w)
                    w[NameObject("/AS")] = on_name if raw_value else NameObject("/Off")
                    if "/V" in w:
                        w[NameObject("/V")] = on_name if raw_value else NameObject("/Off")
                
                parent_on = _on_state_name(widgets[0]) if widgets else NameObject("/Yes")
                field[NameObject("/V")] = parent_on if raw_value else NameObject("/Off")
                logging.info(f"✓ Checkbox set: {field_name} = {raw_value}")
            else:
                target_value = str(raw_value).strip()
                target_name = NameObject("/Off") if is_off_string else _as_name(target_value)
                field[NameObject("/V")] = target_name
                
                matched = False
                for w in widgets:
                    on_name = _on_state_name(w)
                    on_name_str = str(on_name).lstrip("/")
                    target_str = target_value.lstrip("/")
                    
                    if on_name_str == target_str:
                        w[NameObject("/AS")] = on_name
                        if "/V" in w:
                            w[NameObject("/V")] = on_name
                        matched = True
                    else:
                        w[NameObject("/AS")] = NameObject("/Off")
                        if "/V" in w:
                            w[NameObject("/V")] = NameObject("/Off")
                
                if matched:
                    logging.info(f"✓ Radio/Checkbox set: {field_name} = {target_name}")
                else:
                    logging.warning(f"⚠ No match for {field_name} = {target_value}")
        
        # Text fields
        elif ft == NameObject("/Tx"):
            text_value = "" if raw_value is None else str(raw_value)
            field[NameObject("/V")] = TextStringObject(text_value)
            field[NameObject("/DV")] = TextStringObject(text_value)
            logging.info(f"✓ Text field set: {field_name} = '{text_value}'")
        
        # Choice fields
        elif ft == NameObject("/Ch"):
            if isinstance(raw_value, (list, tuple)):
                chosen = "" if not raw_value else str(raw_value[0])
            else:
                chosen = "" if raw_value is None else str(raw_value)
            field[NameObject("/V")] = TextStringObject(chosen)
            field[NameObject("/DV")] = TextStringObject(chosen)
            field[NameObject("/Ff")] = NumberObject(1)
            logging.info(f"✓ Choice field set: {field_name} = '{chosen}'")
    
    # Set NeedAppearances
    acro_form[NameObject("/NeedAppearances")] = BooleanObject(True)
    logging.info("✓ Set NeedAppearances = True")
    
    # Write out
    output_buffer = BytesIO()
    writer.write(output_buffer)
    logging.info("✓ PDF written successfully")
    return output_buffer.getvalue()


def get_pdf_field_names(pdf_bytes: bytes) -> list:
    """Extract ALL field names from PDF template, including nested fields."""
    reader = PdfReader(BytesIO(pdf_bytes))
    field_names = []
    
    def extract_field_info(field_obj, parent_name=""):
        try:
            field_obj = field_obj.get_object() if hasattr(field_obj, 'get_object') else field_obj
            field_name = None
            if "/T" in field_obj:
                field_name = field_obj["/T"]
                full_name = f"{parent_name}.{field_name}" if parent_name else field_name
            else:
                full_name = parent_name
            
            is_terminal = "/V" in field_obj or "/DV" in field_obj
            if is_terminal and full_name:
                field_names.append(full_name)
            
            if "/Kids" in field_obj:
                for kid in field_obj["/Kids"]:
                    extract_field_info(kid, full_name if full_name else "")
        except Exception as e:
            logging.warning(f"Error extracting field info: {e}")
    
    # Extract from AcroForm Fields
    try:
        if "/AcroForm" in reader.trailer["/Root"]:
            acro_form = reader.trailer["/Root"]["/AcroForm"]
            if hasattr(acro_form, 'get_object'):
                acro_form = acro_form.get_object()
            if "/Fields" in acro_form:
                for field in acro_form["/Fields"]:
                    extract_field_info(field)
    except Exception as e:
        logging.warning(f"Error extracting from AcroForm: {e}")
    
    # Extract from page annotations
    for page_num, page in enumerate(reader.pages):
        if "/Annots" in page:
            for annot_ref in page["/Annots"]:
                try:
                    annot = annot_ref.get_object()
                    if annot.get("/Subtype") == "/Widget" and "/T" in annot:
                        field_name = annot["/T"]
                        if field_name not in field_names:
                            field_names.append(field_name)
                except Exception as e:
                    logging.warning(f"Error extracting from annotation: {e}")
    
    logging.info(f"Found {len(field_names)} total fields")
    return field_names


def flatten_pdf_pypdf(pdf_bytes: bytes) -> bytes:
    """Flatten PDF using pypdf only. Removes form fields to prevent editing."""
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        writer = PdfWriter()
        
        for page in reader.pages:
            writer.add_page(page)
        
        if "/AcroForm" in writer._root_object:
            del writer._root_object[NameObject("/AcroForm")]
            logging.info("✓ Removed AcroForm - PDF is now flattened")
        
        output_buffer = BytesIO()
        writer.write(output_buffer)
        return output_buffer.getvalue()
        
    except Exception as e:
        logging.error(f"Error flattening PDF with pypdf: {e}")
        return pdf_bytes
