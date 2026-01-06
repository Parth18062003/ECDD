from typing import Dict, Any, List, Optional
import io, yaml, requests, json, re, uuid, os, time, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import streamlit as st
import pandas as pd
from datetime import datetime, timezone
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
from databricks.sdk import WorkspaceClient
from databricks import sql
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI

st.set_page_config(page_title="Onboarding AI – AML Screening", layout="wide")

st.markdown("""
<style>
body,.stApp{background:#0e1117;color:#e5e7eb}
.header{padding:26px;border-radius:20px;background:linear-gradient(135deg,#020617,#111827);
box-shadow:0 14px 40px rgba(0,0,0,.7)}
.kpi{padding:22px;border-radius:18px;background:rgba(22,27,34,.75);
backdrop-filter:blur(14px);border:1px solid rgba(255,255,255,.08);
box-shadow:0 10px 34px rgba(0,0,0,.6);text-align:center}
.low{color:#22c55e;font-weight:800}
.medium{color:#facc15;font-weight:800}
.high{color:#ef4444;font-weight:800}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(239,68,68,.7)}
70%{box-shadow:0 0 0 18px rgba(239,68,68,0)}
100%{box-shadow:0 0 0 0 rgba(239,68,68,0)}}
.risk-high{animation:pulse 2s infinite}
.risk-medium{box-shadow:0 0 0 8px rgba(250,204,21,.35)}
</style>
""", unsafe_allow_html=True)

# CONFIG
CONFIG = {

    
    # Email Alert Configuration (Free SMTP - Gmail)
    "email_enabled": False,  # Set to True to enable email alerts
    "email_smtp_server": "smtp.gmail.com",
    "email_smtp_port": 587,
    "email_sender": "",  # Your Gmail address
    "email_password": "yfjj vorl xvsq oonn",  # Gmail App Password (not regular password)
    "email_recipient": "",  # CA email to receive alerts
}

INTERNAL_WATCHLIST_TABLE = "dbx_main.ca_bronze.cf_watchlist"
INTERNAL_WATCHLIST_NAME_COL = "Name"
w = WorkspaceClient()

# ============================================================
# UTILITY FUNCTIONS (Reusable across all modules)
# ============================================================

def escape_sql(val) -> str:
    """Escape single quotes for SQL strings. Unified utility for all modules."""
    return (str(val) if val else "").replace("'", "''")

# ============================================================
# EMAIL ALERT FUNCTIONS (CA Notifications)
# ============================================================

def send_email_alert(subject: str, body: str) -> bool:
    """Send an email alert to CA. Returns True on success, False on failure."""
    if not CONFIG.get("email_enabled"):
        return False
    try:
        msg = MIMEMultipart()
        msg["From"] = CONFIG["email_sender"]
        msg["To"] = CONFIG["email_recipient"]
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))
        
        with smtplib.SMTP(CONFIG["email_smtp_server"], CONFIG["email_smtp_port"]) as server:
            server.starttls()
            server.login(CONFIG["email_sender"], CONFIG["email_password"])
            server.send_message(msg)
        return True
    except Exception:
        return False  # Silently fail - don't break main workflow

def notify_module_complete(module: str, case_id: str, customer: str, status: str = "Completed"):
    """Send email notification when a module completes."""
    subject = f"[Onboarding AI] {module} - {status}"
    body = f"""
    <h2>Module Completion Alert</h2>
    <table style="border-collapse:collapse;">
        <tr><td style="padding:8px;border:1px solid #ddd;"><strong>Module:</strong></td><td style="padding:8px;border:1px solid #ddd;">{module}</td></tr>
        <tr><td style="padding:8px;border:1px solid #ddd;"><strong>Customer:</strong></td><td style="padding:8px;border:1px solid #ddd;">{customer}</td></tr>
        <tr><td style="padding:8px;border:1px solid #ddd;"><strong>Case ID:</strong></td><td style="padding:8px;border:1px solid #ddd;">{case_id[:12]}...</td></tr>
        <tr><td style="padding:8px;border:1px solid #ddd;"><strong>Status:</strong></td><td style="padding:8px;border:1px solid #ddd;">{status}</td></tr>
        <tr><td style="padding:8px;border:1px solid #ddd;"><strong>Time:</strong></td><td style="padding:8px;border:1px solid #ddd;">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
    </table>
    <p>Please review the results in the Onboarding AI dashboard.</p>
    """
    send_email_alert(subject, body)

# ============================================================
# REAL-TIME ACTIVITY TRACKER (Main UI - Live Updates)
# ============================================================

_LOG_KEY = "ca_activity_log"
_CLIENT_STATUS_KEY = "ca_client_status"
_STATUS_CONTAINER_KEY = "ca_status_container"

def init_activity_log():
    """Initialize session state for activity logging."""
    if _LOG_KEY not in st.session_state:
        st.session_state[_LOG_KEY] = []
    if _CLIENT_STATUS_KEY not in st.session_state:
        st.session_state[_CLIENT_STATUS_KEY] = ""

def log_activity(module: str, message: str, client_status: str = ""):
    """Add a log entry for CA visibility. Non-technical, progress-focused."""
    init_activity_log()
    entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "module": module,
        "message": message
    }
    st.session_state[_LOG_KEY].append(entry)
    if client_status:
        st.session_state[_CLIENT_STATUS_KEY] = client_status

def clear_activity_log():
    """Clear all activity log entries."""
    st.session_state[_LOG_KEY] = []
    st.session_state[_CLIENT_STATUS_KEY] = ""

def render_activity_log():
    """Render the activity log in the main UI (not sidebar)."""
    init_activity_log()
    logs = st.session_state[_LOG_KEY]
    client_status = st.session_state[_CLIENT_STATUS_KEY]
    
    if not logs and not client_status:
        return
    
    # Client Status Banner - prominent display
    if client_status:
        status_color = "#22c55e" if "Complete" in client_status else "#3b82f6"
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {status_color}22, {status_color}11); 
                    border-left: 4px solid {status_color}; padding: 12px 16px; border-radius: 8px; margin-bottom: 16px;">
            <div style="font-size: 12px; color: #9ca3af; text-transform: uppercase; letter-spacing: 1px;">Client Journey Status</div>
            <div style="font-size: 18px; font-weight: 600; color: #e5e7eb; margin-top: 4px;">📍 {client_status}</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Recent Activity - collapsible
    with st.expander("📋 Recent Agent Activity", expanded=True):
        if logs:
            for entry in reversed(logs[-5:]):  # Reduced from 8 to 5
                st.markdown(f"• `{entry['time']}` {entry['message']}")

class LiveStatus:
    """Context manager for real-time status updates in main UI. Optimized for performance."""
    
    def __init__(self, title: str, module: str):
        self.title = title
        self.module = module
        self.status = None
        self.logs = []
        self.final_client_status = ""
        
    def __enter__(self):
        self.status = st.status(self.title, expanded=True)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Batch save all logs to session state at the end (performance optimization)
        if self.logs:
            init_activity_log()
            for entry in self.logs:
                st.session_state[_LOG_KEY].append(entry)
            if self.final_client_status:
                st.session_state[_CLIENT_STATUS_KEY] = self.final_client_status
        
        if exc_type is None:
            self.status.update(label=f"✅ {self.title}", state="complete", expanded=False)
        else:
            self.status.update(label=f"❌ {self.title} - Error", state="error", expanded=True)
        return False
    
    def log(self, message: str, client_status: str = ""):
        """Log a message and display it in real-time. Session state saved at end."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append({"time": timestamp, "module": self.module, "message": message})
        if client_status:
            self.final_client_status = client_status
        
        # Only update UI, skip session state save (done in __exit__)
        with self.status:
            st.write(f"🔹 {message}")



# ============================================================
# DATABRICKS OAUTH TOKEN (Unified auth / Apps auth)
# ============================================================
_oauth_cache = {"token": None, "ts": 0.0}


def get_databricks_oauth_token() -> str:
    """Return a workspace OAuth access token using Databricks unified authentication.

    In Databricks Apps, this resolves to the app/user OAuth context.
    Locally, it resolves via Databricks CLI unified auth (if configured).
    """
    global _oauth_cache
    now = time.time()
    # Tokens are typically ~1 hour; refresh a bit early.
    if _oauth_cache.get("token") and (now - float(_oauth_cache.get("ts", 0.0)) < 3300):
        return _oauth_cache["token"]

    host = CONFIG.get("workspace_url", "").rstrip("/")
    wc = WorkspaceClient(host=host) if host else WorkspaceClient()
    auth_header = wc.config.authenticate()  # {"Authorization": "Bearer <token>"}
    token = auth_header["Authorization"].split()[1]
    _oauth_cache = {"token": token, "ts": now}
    return token

def databricks_auth_headers(content_type: str = "application/json") -> dict:
    return {
        "Authorization": f"Bearer {get_databricks_oauth_token()}",
        "Content-Type": content_type,
    }

# SAFE JSON PARSER
def safe_json_from_llm(raw: str):
    raw = raw.strip().replace("```json", "").replace("```", "")
    decoder = json.JSONDecoder()
    for i, ch in enumerate(raw):
        if ch == "{":
            try:
                obj, _ = decoder.raw_decode(raw[i:])
                return obj
            except json.JSONDecodeError:
                continue
    raise ValueError("No valid JSON found")

# DOCUMENT INTELLIGENCE
@st.cache_data(ttl=3600)
def extract_text_from_pdf(pdf_bytes):
    client = DocumentIntelligenceClient(
        CONFIG["endpoint"],
        AzureKeyCredential(CONFIG["api_key"])
    )
    poller = client.begin_analyze_document("prebuilt-layout", body=pdf_bytes)
    return poller.result().content or ""

# LLM CALL
def call_model(prompt):
    r = requests.post(
        f"{CONFIG['workspace_url']}/serving-endpoints/{CONFIG['model_endpoint']}/invocations",
        headers=databricks_auth_headers(),
        json={
            "messages": [
                {"role": "system", "content": "You are an AML compliance analyst."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 2048
        },
        timeout=30
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

# DOCUMENT TYPE DETECTION
def detect_document_type(text):
    raw = call_model(f"""
You are a document classifier. Read the text and decide which document type it is.

You MUST answer with EXACTLY one of these:
- Bank Customer Form
- Bank Statement
- Self Declaration Form

Document Type Definitions:

Bank Customer Form:
- Contains fields like Applicant Name, Address, Email, Phone
- Looks like a form with labeled input fields or blanks to be filled
- Does NOT contain transaction tables

Bank Statement:
- Contains financial transactions in table form (dates, credits, debits)
- Contains account number, bank name, statement period, balances
- Includes rows of monetary values and totals

Self Declaration Form:
- Contains paragraphs like “I hereby declare...” or “This is to certify...”
- Written in prose, not table format
- Includes signature, date, purpose of declaration
TEXT:
{text}
""")
    if "Bank Statement" in raw:
        return "Bank Statement"
    if "Self Declaration" in raw:
        return "Self Declaration Form"
    return "Bank Customer Form"

# FIELD EXTRACTION
def extract_fields(text, doc_type):
    schemas = {
        "Bank Customer Form": '{"applicant_name":"","full_address":"","phone_number":"","email":"","amount":"","beneficiary_names":[]}',
        "Bank Statement": '{"account_holder_name":"","account_number":"","bank_name":"","statement_period":"","total_deposits":"","total_withdrawals":""}',
        "Self Declaration Form": '{"declarant_name":"","purpose_of_declaration":"","address":"","date":"","signature_present":""}'
    }
    raw = call_model(f"Extract JSON:\n{schemas[doc_type]}\nTEXT:\n{text}")
    return safe_json_from_llm(raw)

# SAVE TO DELTA
def save_to_delta(fields, doc_type, case_id):
    table = CONFIG["delta_tables"][doc_type]
    record = fields.copy()
    record["case_id"] = case_id
    record["created_at"] = datetime.now(timezone.utc).isoformat()

    cols = ", ".join(record.keys())
    vals = ", ".join([f"'{escape_sql(v)}'" for v in record.values()])

    execute_sql(f"INSERT INTO {table} ({cols}) VALUES ({vals})")


def save_aml_report(case_id, customer_name, document_type, aml_report):
    aml_json = escape_sql(json.dumps(aml_report))

    statement = f"""
        INSERT INTO dbx_main.ca_bronze.preliminarysearch_results
        (case_id, customer_name, document_type, risk_score, aml_json, created_at)
        VALUES (
            '{escape_sql(case_id)}',
            '{escape_sql(customer_name)}',
            '{escape_sql(document_type)}',
            '{escape_sql(aml_report.get("risk_score", "REVIEW"))}',
            '{aml_json}',
            current_timestamp()
        )
    """

    execute_sql(statement)

# FETCH NAME
def fetch_name(doc_type, case_id):
    table = CONFIG["delta_tables"][doc_type]

    # Explicit name column per document type
    name_column_map = {
        "Bank Customer Form": "applicant_name",
        "Bank Statement": "account_holder_name",
        "Self Declaration Form": "declarant_name"
    }

    name_col = name_column_map[doc_type]

    result = execute_sql(f"""
SELECT {name_col} FROM {table} WHERE case_id = '{case_id}' LIMIT 1
""")
    rows = result.get('result', {}).get('data_array', [])
    value = rows[0][0] if rows else None

    if not value or not str(value).strip():
        raise Exception(f"{name_col} is empty – cannot run AML")

    return value.strip()

# AML TOOLS
def tool_opensanctions(name):
    try:
        return requests.get(f"https://api.opensanctions.org/search?q={name}", timeout=10).json().get("results", [])
    except:
        return []


def tool_un(name):
    try:
        return name.lower() in requests.get("https://scsanctions.un.org/resources/xml/en/consolidated.xml").text.lower()
    except:
        return False


def tool_ofac(name):
    try:
        return name.lower() in requests.get("https://ofac.treasury.gov/media/7761/download").text.lower()
    except:
        return False


def tool_eu(name):
    try:
        data = requests.get(
            "https://webgate.ec.europa.eu/fsd/fsf/public/files/json/FSF_JSON.json").json()
        return [x["name"] for x in data.get("result", []) if name.lower() in x["name"].lower()]
    except:
        return []


def tool_interpol(name):
    try:
        r = requests.get(
            f"https://ws-public.interpol.int/notices/v1/red?name={name}").json()
        return r.get("_embedded", {}).get("notices", [])
    except:
        return []


def tool_adverse_news(name):
    try:
        html = requests.get(
            f"https://www.google.com/search?q={name}+fraud+scam",
            headers={"User-Agent": "Mozilla/5.0"}
        ).text
        return re.findall(r"<h3.*?>(.*?)</h3>", html)[:5]
    except:
        return []


def tool_internal_watchlist(name):
    try:
        statement = f"""
            SELECT Name, RiskType, SourceList, Status
            FROM {INTERNAL_WATCHLIST_TABLE}
            WHERE lower(trim(Name)) LIKE lower('%{name}%')
              AND Status = 'Active'
            LIMIT 1
        """

        result = execute_sql(statement)
        rows = result.get('result', {}).get('data_array', [])

        if not rows:
            return {
                "listed": False
            }

        # Column order: Name, RiskType, SourceList, Status
        row = rows[0]
        return {
            "listed": True,
            "risk_type": row[1],
            "source_list": row[2],
            "status": row[3]
        }

    except Exception as e:
        return {
            "listed": False,
            "error": str(e)
        }

# AML REPORT
def extract_risk_score(report):
    # 1️⃣ Try nested AML path first
    try:
        return report["aml_kyc_report"]["conclusion_and_risk_score"]["risk_score"].upper()
    except Exception:
        pass

    # 2️⃣ Try top-level if it exists
    if isinstance(report.get("risk_score"), str):
        rs = report["risk_score"].upper()
        if rs in ["LOW", "MEDIUM", "HIGH"]:
            return rs

    # 3️⃣ Fallback
    return "REVIEW"


def agentic_aml_screen(name):
    memory = {
        "OpenSanctions": tool_opensanctions(name),
        "UN": tool_un(name),
        "OFAC": tool_ofac(name),
        "EU": tool_eu(name),
        "Interpol": tool_interpol(name),
        "Adverse News": tool_adverse_news(name),
        "Internal Watchlist": tool_internal_watchlist(name)
    }

    raw = call_model(f"""
Generate a complete AML/KYC Report for customer: {name}

Input Findings:
{json.dumps(memory, indent=2)}

Your report MUST contain:
- Sanctions summary
- PEP risk assessment
- Interpol notices
- Adverse news summary
- Connected persons
- Conclusion and Risk Score: LOW / MEDIUM / HIGH

Return full report as clean JSON.
""")

    report = safe_json_from_llm(raw)
    report["risk_score"] = extract_risk_score(report)
    return report

# ECDD Module
ECDD_CONFIG = {
    "aml_source_table": "dbx_main.ca_bronze.preliminarysearch_results",
    "ecdd_reports_table": "dbx_main.ca_bronze.ecdd_reports",
    "volumes_path": "/Volumes/dbx_main/ca_bronze/ecdd_pdfs",
    "local_fallback_path": "./exports"
}

# ECDD DATA CLASSES 
class QuestionField:
    """Individual question in a questionnaire."""

    def __init__(self, data: dict):
        self.field_id = data.get("field_id", str(uuid.uuid4())[:8])
        self.question_text = data.get("question_text", "")
        self.question_type = data.get("question_type", "text")
        self.required = data.get("required", True)
        self.help_text = data.get("help_text", "")
        self.options = data.get("options", [])
        self.category = data.get("category", "")
        self.aml_relevant = data.get("aml_relevant", False)


class QuestionSection:
    """Section containing multiple questions."""

    def __init__(self, data: dict):
        self.section_id = data.get("section_id", str(uuid.uuid4())[:8])
        self.section_title = data.get("section_title", "Questions")
        self.section_description = data.get("section_description", "")
        self.section_icon = data.get("section_icon", "📋")
        self.order = data.get("order", 0)
        self.questions = [QuestionField(q) for q in data.get("questions", [])]


class DynamicQuestionnaire:
    """Full dynamic questionnaire with sections."""

    def __init__(self, data: dict, customer_id: str = "", customer_name: str = ""):
        self.questionnaire_id = str(uuid.uuid4())
        self.customer_id = customer_id
        self.customer_name = customer_name
        self.client_type = data.get("client_type", "Individual")
        self.sections = [QuestionSection(s) for s in data.get("sections", [])]
        self.profile_summary = data.get("profile_summary", {})
        self.created_at = datetime.now(timezone.utc)

    def all_questions(self) -> List[QuestionField]:
        """Get all questions across all sections."""
        questions = []
        for section in self.sections:
            questions.extend(section.questions)
        return questions


class DocumentItem:
    """Single document in the checklist."""

    def __init__(self, data: dict):
        self.document_name = data.get("document_name", "")
        self.priority = data.get("priority", "required")
        self.category = data.get("category", "")
        self.special_instructions = data.get("special_instructions", "")


class DocumentChecklist:
    """Document checklist generated by reporter."""

    def __init__(self, data: dict = None):
        data = data or {}
        self.identity_documents = [DocumentItem(
            d) for d in data.get("identity_documents", [])]
        self.source_of_wealth_documents = [DocumentItem(
            d) for d in data.get("source_of_wealth_documents", [])]
        self.source_of_funds_documents = [DocumentItem(
            d) for d in data.get("source_of_funds_documents", [])]
        self.compliance_documents = [DocumentItem(
            d) for d in data.get("compliance_documents", [])]
        self.additional_documents = [DocumentItem(
            d) for d in data.get("additional_documents", [])]

    def all_documents(self) -> List[DocumentItem]:
        return (self.identity_documents + self.source_of_wealth_documents +
                self.source_of_funds_documents + self.compliance_documents + self.additional_documents)


class ComplianceFlags:
    """Compliance flags from assessment."""

    def __init__(self, data: dict = None):
        data = data or {}
        self.pep = data.get("pep", False)
        self.sanctions = data.get("sanctions", False)
        self.adverse_media = data.get("adverse_media", False)
        self.high_risk_jurisdiction = data.get("high_risk_jurisdiction", False)
        self.watchlist_hit = data.get("watchlist_hit", False)
        self.source_of_wealth_concerns = data.get(
            "source_of_wealth_concerns", False)
        self.source_of_funds_concerns = data.get(
            "source_of_funds_concerns", False)
        self.complex_ownership = data.get("complex_ownership", False)

    def to_dict(self) -> dict:
        return {
            "pep": self.pep,
            "sanctions": self.sanctions,
            "adverse_media": self.adverse_media,
            "high_risk_jurisdiction": self.high_risk_jurisdiction,
            "watchlist_hit": self.watchlist_hit,
            "source_of_wealth_concerns": self.source_of_wealth_concerns,
            "source_of_funds_concerns": self.source_of_funds_concerns,
            "complex_ownership": self.complex_ownership
        }


class ECDDAssessment:
    """Full ECDD assessment report."""

    def __init__(self):
        self.client_type = ""
        self.client_category = ""
        self.overall_ecdd_level = "MEDIUM"
        self.source_of_wealth = {}
        self.source_of_funds = {}
        self.compliance_flags = ComplianceFlags()
        self.recommendations = []
        self.required_actions = []
        self.report_text = ""
        self.rm_guidance = ""

    def to_dict(self) -> dict:
        return {
            "client_type": self.client_type,
            "client_category": self.client_category,
            "overall_ecdd_level": self.overall_ecdd_level,
            "source_of_wealth": self.source_of_wealth,
            "source_of_funds": self.source_of_funds,
            "compliance_flags": self.compliance_flags.to_dict(),
            "recommendations": self.recommendations,
            "required_actions": self.required_actions,
            "report_text": self.report_text,
            "rm_guidance": self.rm_guidance
        }

# READ AML TABLE
_db_connection = None

def get_databricks_connection():
    """
    Get or create a Databricks SQL connection using the Apps workspace OAuth token.
    """
    global _db_connection
    if _db_connection is None:
        try:
            from databricks import sql
            from databricks.sdk import WorkspaceClient
            # 1) Resolve host & http_path (from CONFIG or env)
            host = CONFIG.get("databricks_host", os.environ.get("DATABRICKS_HOST", "adb-.15.azuredatabricks.net"))
            http_path = CONFIG.get("http_path", os.environ.get("DATABRICKS_HTTP_PATH", "sql/protocolv1/o//1216-071746-h1nsb1l2"))
            if not host or not http_path:
                raise ValueError("Missing DATABRICKS_HOST / DATABRICKS_HTTP_PATH")

            host = host.rstrip("/")
            server_hostname = host.replace("https://", "").replace("http://", "")

            wc = WorkspaceClient(host=host)
            auth_header = wc.config.authenticate()                 
            access_token = auth_header["Authorization"].split()[1]  

            _db_connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token,
            )
        except ImportError:
            raise ImportError(
                "databricks-sql-connector not installed. Run: pip install databricks-sql-connector"
            )
        except Exception as e:
            raise Exception(f"Failed to connect via SDK token: {e}")
    return _db_connection

def execute_sql_cli(statement: str) -> list:
    """
    Execute SQL using Databricks CLI authentication (databricks-sql-connector).
    Returns list of dicts with column names as keys.
    """
    conn = get_databricks_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(statement)
        columns = [desc[0]
                   for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()

def execute_sql(statement: str) -> dict:
    """Execute SQL statement against Databricks using the SQL connector over a shared cluster."""
    rows = execute_sql_cli(statement)
    if not rows:
        return {"result": {"data_array": [], "schema": {"columns": []}}}
    columns = list(rows[0].keys()) if rows else []
    data_array = [[row.get(c) for c in columns] for row in rows]
    return {"result": {"data_array": data_array, "schema": {"columns": [{"name": c} for c in columns]}}}


@st.cache_data(ttl=300)
def load_aml_cases():
    """Load AML cases from Delta table."""
    statement = f"""
        SELECT
            case_id,
            customer_name,
            document_type,
            risk_score,
            aml_json,
            created_at
        FROM {ECDD_CONFIG['aml_source_table']}
        ORDER BY created_at DESC
        LIMIT 100
    """
    result = execute_sql(statement)
    #result = execute_sql_pat(statement)
    if "error" in result:
        raise Exception(f"SQL Error: {result['error']}")
    if "result" not in result:
        raise Exception(f"No result returned: {result}")

    rows = result["result"].get("data_array", [])
    if "schema" in result["result"]:
        cols = [c["name"] for c in result["result"]["schema"]["columns"]]
    else:
        cols = ["case_id", "customer_name", "document_type",
                "risk_score", "aml_json", "created_at"]

    return pd.DataFrame(rows, columns=cols)

def generate_ecdd_questionnaire(aml_json: str, customer_name: str = "", case_id: str = "") -> DynamicQuestionnaire:
    """
    Generate dynamic ECDD questionnaire using LLM based on AML findings.
    Uses the full questionnaire_agent.py prompt for profile-aware questions.
    Includes fallback questions when client type cannot be determined.
    """
    prompt = f"""
You are an intelligent ECDD (Enhanced Client Due Diligence) Data Collection System.
Your goal is to generate a personalized questionnaire for the CLIENT to complete based on their specific profile.

DYNAMIC QUESTIONNAIRE LOGIC (MANDATORY)
You must analyze the client's profile and dynamically determine the content of the questionnaire. 

1.  **Profile Inference:**
    Analyze the client's occupation, employment status, and source of income to determine their primary archetype 
    (e.g., Employee, Business Owner, Trust Beneficiary).

2.  **IMPORTANT - When Client Type is Unclear:**
    If you CANNOT confidently determine the client type from the AML data, you MUST:
    - Set client_type to "Unknown - Requires Clarification"
    - Include a FIRST section called "Client Profile Clarification" with these mandatory questions:
      a) "Which of the following best describes your primary occupation?" (dropdown: Employed, Self-Employed/Business Owner, Trust Beneficiary)
      b) "If employed, please provide your current employer name and your role/title" (text)
      c) "If self-employed or a business owner, please describe your business and your role" (textarea)
      d) "What is your primary source of income?" (dropdown: Salary/Wages, Business Profits, Investments/Dividends, Rental Income, Pension, Inheritance, Trust Distributions, Other)
      e) "Please briefly describe how you accumulated your current wealth over your career" (textarea)

3.  **Contextual Section Generation:**
    Create 4-6 sections that make sense for this specific archetype.
    • If the client is a **Business Owner/Entrepreneur**: Focus sections on "Business Activities," "Ownership Structure," and "Company Details." 
      Do NOT ask for "Employer Name" or "Employment History."
    • If the client is an **Employee**: Focus sections on "Employment Details," "Role," and "Remuneration." 
      Do NOT ask for "Company Financials" or "Shareholding structure" unless relevant.
    • If the client is a **Trust Beneficiary**: Focus sections on "Trust Structure," "Settlors," and "Trustees."

4.  **Irrelevance Filtering:**
    Strictly exclude any questions that are logically incompatible with the client's status. 

CORE PRINCIPLES
1.  **DATA COLLECTION ONLY:** You are collecting data for ECDD verification. Do NOT perform risk assessments.
2.  **AUDIENCE:** Address the client directly using "you" and "your."
3.  **CONVERSATIONAL TONE:** Questions should feel like a natural conversation, not an interrogation.
4.  **INFORMATION GAPS:** Do not ask for data already explicitly present in the provided profile.
5.  **SCOPE:** Focus on Identity, Source of Wealth (SoW), Source of Funds (SoF), Financial Position, and Compliance Declarations.

INPUT DATA:
Customer Name: {customer_name}
AML Screening Results:
{aml_json}

STRICT CONSTRAINTS
• **Total Questions:** 15 to 25 questions.
• **Section Count:** 4 to 6 sections.
• **JSON Format:** Valid JSON only. No markdown blocks.
• **Always include descriptive help_text** for each question to guide the client.

OUTPUT FORMAT (JSON only):
{{
  "client_type": "Inferred Type OR 'Unknown - Requires Clarification' if unclear",
  "sections": [
    {{
      "section_id": "string_unique_id",
      "section_title": "string (Context-aware, e.g., Business Ownership)",
      "section_description": "string (Why this is relevant and what information we need)",
      "section_icon": "emoji",
      "order": 1,
      "questions": [
        {{
          "field_id": "string_unique_field_id",
          "question_text": "string (Direct, conversational question to the client)",
          "question_type": "text | textarea | dropdown | multiple_choice | checkbox | date | number | currency | yes_no",
          "required": true,
          "help_text": "string (Helpful guidance on how to answer)",
          "options": ["Option A", "Option B"],
          "category": "identity | sow | sof | business | financial | compliance",
          "aml_relevant": true
        }}
      ]
    }}
  ]
}}

Note: The "options" key should only be present if question_type is 'dropdown', 'multiple_choice', or 'checkbox'.
Return JSON only. No markdown. No commentary.
"""
    raw = call_model(prompt)
    data = safe_json_from_llm(raw)
    return DynamicQuestionnaire(data, customer_id=case_id, customer_name=customer_name)

def generate_ecdd_assessment(
    customer_name: str,
    aml_json: str,
    responses: Dict[str, str],
    case_id: str = ""
) -> tuple:
    """
    Generate full ECDD assessment report, document checklist, and RM guidance using LLM.
    Uses the reporter_agent.py prompt for comprehensive output with detailed narratives.
    Returns: (ECDDAssessment, DocumentChecklist)
    """
    # Format responses for prompt with question context
    responses_text = "\n".join(
        [f"- {k}: {v}" for k, v in responses.items() if v])

    prompt = f"""
You are an ECDD (Enhanced Client Due Diligence) Data Verification and Reporting Specialist for a major international bank.

Your role is to analyze questionnaire responses and generate COMPREHENSIVE, DETAILED outputs:
1. A thorough ECDD Summary Report with detailed narrative sections
2. A structured document checklist with specific instructions
3. Compliance status flags for factual screening (PEP, Sanctions, Adverse Media, etc.)
4. Detailed guidance for the Relationship Manager (RM)

CRITICAL REQUIREMENTS FOR DESCRIPTIVE OUTPUT:
• Each section MUST contain multiple sentences with specific details
• Use professional banking language and terminology
• Include specific observations from the data provided
• Provide reasoning for risk classifications
• Be thorough - this report will be used for regulatory compliance

INPUT DATA:
Customer: {customer_name}
Case ID: {case_id}

AML SCREENING RESULTS:
{aml_json}

QUESTIONNAIRE RESPONSES:
{responses_text if responses_text else "No responses provided yet."}

ECDD SUMMARY REPORT FORMAT (DETAILED):
Generate a professional, comprehensive summary report with these sections:

1. CLIENT IDENTIFICATION
   - Full name, aliases, date of birth, nationality, residency
   - Unique identifiers verified
   - Current status and relationship with the bank

2. CLIENT TYPE CLASSIFICATION
   - Classification rationale based on occupation, income sources, and business activities
   - Why this classification was determined
   - Any dual classifications if applicable

3. SOURCE OF WEALTH (SOW) ANALYSIS
   - Detailed narrative of how the client accumulated their wealth over time
   - Career progression and income history
   - Major assets and how they were acquired
   - Inheritance, gifts, or windfalls if applicable
   - Assessment of whether SOW is adequately explained

4. SOURCE OF FUNDS (SOF) ANALYSIS
   - Current and ongoing sources of funds
   - Employment income details with figures if provided
   - Business income with nature of business
   - Investment income and portfolio details
   - Any other fund sources
   - Assessment of whether SOF is adequately explained

5. COMPLIANCE SCREENING RESULTS
   - PEP Status: Detailed explanation if PEP or connected to PEP
   - Sanctions Screening: Results and any matches found
   - Adverse Media: Summary of any negative news findings
   - High-Risk Jurisdiction Exposure: Countries involved and risk level
   - Watchlist Status: Any matches and their relevance

6. RISK ASSESSMENT RATIONALE
   - Why the overall risk level was determined
   - Key factors contributing to risk classification
   - Mitigating factors if any

7. RECOMMENDATIONS & NEXT STEPS
   - Specific actions required before account approval/continuation
   - Documents needed with priority levels
   - Questions for follow-up with client
   - Ongoing monitoring requirements

OUTPUT AS TWO JSON BLOCKS:

FIRST JSON BLOCK - ASSESSMENT:
{{
    "client_type": "High Net Worth Individual | Corporate Entity | Trust | Private Banking Client | etc.",
    "client_category": "Individual | Corporate | Trust | Foundation | etc.",
    "overall_ecdd_level": "LOW | MEDIUM | HIGH",
    "compliance_flags": {{
        "pep": true/false,
        "sanctions": true/false,
        "adverse_media": true/false,
        "high_risk_jurisdiction": true/false,
        "watchlist_hit": true/false,
        "source_of_wealth_concerns": true/false,
        "source_of_funds_concerns": true/false,
        "complex_ownership": true/false
    }},
    "source_of_wealth": {{
        "summary": "Comprehensive multi-paragraph narrative detailing how the client accumulated their wealth. Include career history, asset acquisition timeline, inheritance details, and any major wealth events. This should be 3-5 sentences minimum with specific details from the questionnaire responses."
    }},
    "source_of_funds": {{
        "summary": "Comprehensive multi-paragraph narrative detailing the client's current sources of funds. Include employment income, business profits, investment returns, rental income, etc. This should be 3-5 sentences minimum with specific details."
    }},
    "recommendations": [
        "Specific recommendation 1 with context",
        "Specific recommendation 2 with context",
        "Specific recommendation 3 with context"
    ],
    "required_actions": [
        "Mandatory action 1 before proceeding",
        "Mandatory action 2 before proceeding"
    ],
    "report_text": "FULL FORMAL NARRATIVE REPORT - This should be a complete, multi-paragraph professional report suitable for regulatory review. Include all seven sections above in a flowing narrative format. Each section should have a header and detailed content. Aim for 500+ words covering all aspects of the ECDD review. Use formal banking language.",
    "rm_guidance": "DETAILED RELATIONSHIP MANAGER GUIDANCE - Provide comprehensive guidance including: (1) Key talking points for client discussion, (2) Specific areas requiring clarification, (3) Red flags to monitor, (4) Suggested questions for follow-up meeting, (5) Timeline recommendations, (6) Escalation triggers if applicable. This should be 4-6 sentences minimum with actionable items."
}}

SECOND JSON BLOCK - DOCUMENT CHECKLIST:
{{
    "identity_documents": [
        {{"document_name": "Valid government-issued photo ID (passport/national ID)", "priority": "required", "category": "identity", "special_instructions": "Must be current and unexpired. Verify photo matches client. Check for signs of tampering."}}
    ],
    "source_of_wealth_documents": [
        {{"document_name": "Employment contracts and salary statements for past 3 years", "priority": "required", "category": "sow", "special_instructions": "Should show progression of income. Verify employer legitimacy independently."}}
    ],
    "source_of_funds_documents": [
        {{"document_name": "Last 3 months bank statements showing regular income deposits", "priority": "required", "category": "sof", "special_instructions": "Ensure all large deposits are explainable. Check for unusual patterns."}}
    ],
    "compliance_documents": [
        {{"document_name": "Signed FATCA/CRS self-certification form", "priority": "required", "category": "compliance", "special_instructions": "Ensure all sections completed. Verify tax residency claims."}}
    ],
    "additional_documents": [
        {{"document_name": "Proof of address dated within 3 months", "priority": "required", "category": "other", "special_instructions": "Utility bill or bank statement. Must match declared address."}}
    ]
}}

IMPORTANT: Generate detailed, specific content for each field. Do not use generic placeholders.
Return both JSON blocks. No markdown fences.
"""
    raw = call_model(prompt)

    # Extract JSON objects from response
    assessment_data = None
    checklist_data = None

    # Try to extract multiple JSON objects
    decoder = json.JSONDecoder()
    i = 0
    found_objects = []
    while i < len(raw):
        if raw[i] != '{':
            i += 1
            continue
        try:
            obj, end = decoder.raw_decode(raw, i)
            if isinstance(obj, dict):
                found_objects.append((obj, i, end))
            i = end
        except Exception:
            i += 1

    # Identify which object is which
    for obj, start, end in found_objects:
        if assessment_data is None and ("compliance_flags" in obj or "client_type" in obj or "overall_ecdd_level" in obj):
            assessment_data = obj
        elif checklist_data is None and ("identity_documents" in obj or "source_of_wealth_documents" in obj):
            checklist_data = obj

    # Build assessment
    assessment = ECDDAssessment()
    if assessment_data:
        assessment.client_type = assessment_data.get("client_type", "Unknown")
        assessment.client_category = assessment_data.get(
            "client_category", "Individual")
        assessment.overall_ecdd_level = assessment_data.get(
            "overall_ecdd_level", "MEDIUM")
        assessment.compliance_flags = ComplianceFlags(
            assessment_data.get("compliance_flags", {}))
        assessment.source_of_wealth = assessment_data.get(
            "source_of_wealth", {})
        assessment.source_of_funds = assessment_data.get("source_of_funds", {})
        assessment.recommendations = assessment_data.get("recommendations", [])
        assessment.required_actions = assessment_data.get(
            "required_actions", [])
        assessment.report_text = assessment_data.get("report_text", "")
        assessment.rm_guidance = assessment_data.get("rm_guidance", "")

    # Build checklist
    checklist = DocumentChecklist(
        checklist_data) if checklist_data else DocumentChecklist()

    return assessment, checklist


def save_ecdd_to_delta(
    case_id: str,
    customer_name: str,
    assessment: ECDDAssessment,
    checklist: DocumentChecklist,
    responses: Dict[str, str]
) -> str:
    """
    Save structured ECDD output to Delta table.
    Matches the databricks.py write_ecdd_output structure.
    Returns session_id on success, empty string on failure.
    """
    session_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    # Build structured JSON columns - ecdd_level is stored INSIDE ecdd_assessment_json
    compliance_flags_json = escape_sql(json.dumps(assessment.compliance_flags.to_dict()))

    ecdd_assessment_json = escape_sql(json.dumps({
        "client_type": assessment.client_type,
        "client_category": assessment.client_category,
        "overall_ecdd_level": assessment.overall_ecdd_level,
        "source_of_wealth": assessment.source_of_wealth,
        "source_of_funds": assessment.source_of_funds,
        "recommendations": assessment.recommendations,
        "required_actions": assessment.required_actions,
        "report_text": assessment.report_text,
        "rm_guidance": assessment.rm_guidance
    }))

    document_checklist_json = escape_sql(json.dumps({
        "identity_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.identity_documents],
        "source_of_wealth_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.source_of_wealth_documents],
        "source_of_funds_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.source_of_funds_documents],
        "compliance_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.compliance_documents],
        "additional_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.additional_documents]
    }))

    questionnaire_responses_json = escape_sql(json.dumps(responses))

    # Note: No standalone ecdd_level column - it's inside ecdd_assessment_json
    statement = f"""
        INSERT INTO {ECDD_CONFIG['ecdd_reports_table']}
        (session_id, case_id, customer_name, compliance_flags_json, 
         ecdd_assessment_json, document_checklist_json, questionnaire_responses_json, status, created_at, updated_at)
        VALUES (
            '{session_id}',
            '{escape_sql(case_id)}',
            '{escape_sql(customer_name)}',
            '{compliance_flags_json}',
            '{ecdd_assessment_json}',
            '{document_checklist_json}',
            '{questionnaire_responses_json}',
            'completed',
            '{now}',
            '{now}'
        )
    """
    try:
        execute_sql(statement)
        return session_id
    except Exception as e:
        st.error(f"❌ Failed to save to Delta table: {e}")
        return ""
# END of ECDD Module
# START OF RISK ASSESSMENT MODULE
def _get_server_hostname_and_http_path():
    host = CONFIG["workspace_url"].strip().rstrip("/")
    server_hostname = host.replace("https://", "").replace("http://", "")
    http_path = f"/sql/1.0/warehouses/{CONFIG['sql_warehouse_id']}"
    return server_hostname, http_path

def _open_connection_auto():
    """Use OAuth token (same as ECDD) for Databricks SQL warehouse connections. PAT is not used.
    Reuses get_databricks_oauth_token defined earlier."""
    server_hostname, http_path = _get_server_hostname_and_http_path()
    # Obtain the unified OAuth token via WorkspaceClient (same function used by ECDD)
    access_token = get_databricks_oauth_token()
    return sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=access_token)

def sql_to_df(query: str) -> pd.DataFrame:
    with _open_connection_auto() as cn:
        with cn.cursor() as cur:
            cur.execute(query)
            try:
                tbl = cur.fetchall_arrow()
                return tbl.to_pandas()
            except Exception:
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
                return pd.DataFrame(rows, columns=cols)

def sql_execute(query: str) -> None:
    with _open_connection_auto() as cn:
        with cn.cursor() as cur:
            cur.execute(query)

# FIELD EXTRACTORS (AML + questionnaire)
def _ensure_json_obj(v):
    if isinstance(v, dict): return v
    if isinstance(v, str) and v.strip():
        try: return json.loads(v)
        except Exception: return {}
    return {}

def _to_bool(v) -> bool:
    if isinstance(v, bool): return v
    if v is None: return False
    return str(v).strip().lower() in {"true", "yes", "y", "1"}

def _yes_no(flag: bool) -> str:
    return "Yes" if flag else "No"

def _any_hits_from_strings(d: dict) -> bool:
    if not isinstance(d, dict) or not d: return False
    for v in d.values():
        s = str(v).strip().lower()
        if s and not any(neg in s for neg in ["no match","no matches","no adverse","not found","none","no hit"]):
            if s in {"match","hit","listed","active"}:
                return True
    return False

def _extract_fields_from_aml(aml: dict) -> dict:
    r = {
        "Customer_Name": None,
        "Sanctions_Hit": "No",
        "Internal_Watchlist": "No",
        "Is_PEP": "No",
        "Interpol_Notices": "No",
        "Adverse_News": "No",
        "Connected_Persons": "None",
    }
    if not aml: return r
    r["Customer_Name"] = aml.get("CustomerName")
    rep = (aml.get("AMLKYCReport") or {})
    sanc = rep.get("SanctionsSummary") or {}
    results = sanc.get("Results")
    sanctions_check = sanc.get("SanctionsCheck")
    sanctions_hit = False
    if isinstance(results, dict):
        un_hit   = _to_bool(results.get("UN"))
        ofac_hit = _to_bool(results.get("OFAC"))
        eu       = results.get("EU")
        eu_hit   = isinstance(eu, list) and len(eu) > 0
        sanctions_hit = un_hit or ofac_hit or eu_hit
    elif isinstance(sanctions_check, dict):
        sanctions_hit = _any_hits_from_strings(sanctions_check)
    r["Sanctions_Hit"] = _yes_no(sanctions_hit)

    watch = (rep.get("InternalWatchlist") or sanc.get("InternalWatchlist") or {})
    r["Internal_Watchlist"] = _yes_no(_to_bool(watch.get("Listed")))

    pep = rep.get("PEPRiskAssessment") or {}
    is_pep_bool = _to_bool(pep.get("IsPEP"))
    pep_risk = str(pep.get("PEPRisk","")).strip().upper() if pep.get("PEPRisk") is not None else ""
    is_pep = is_pep_bool or (pep_risk in {"HIGH","MEDIUM"})
    r["Is_PEP"] = _yes_no(is_pep)

    interpol = rep.get("InterpolNotices") or {}
    r["Interpol_Notices"] = _yes_no(
        (isinstance(interpol.get("Results"), list) and len(interpol.get("Results")) > 0)
        or _to_bool(interpol.get("InterpolCheck"))
    )

    adverse = rep.get("AdverseNewsSummary") or {}
    r["Adverse_News"] = _yes_no(
        (isinstance(adverse.get("Results"), list) and len(adverse.get("Results")) > 0)
        or _to_bool(adverse.get("AdverseNewsCheck"))
    )

    conn = rep.get("ConnectedPersons") or {}
    names = []
    if isinstance(conn.get("Results"), list):
        for item in conn.get("Results"):
            nm = (item or {}).get("Name")
            if nm: names.append(nm)
    r["Connected_Persons"] = ", ".join(names) if names else "None"
    return r

def _extract_compliance_flags(flags_json: dict) -> dict:
    if not isinstance(flags_json, dict): flags_json = {}
    return {
        "High_Risk_Jurisdiction": _yes_no(_to_bool(flags_json.get("high_risk_jurisdiction"))),
        "Watchlist_Hit":          _yes_no(_to_bool(flags_json.get("watchlist_hit"))),
        "Source_of_Wealth_Concerns": _yes_no(_to_bool(flags_json.get("source_of_wealth_concerns"))),
        "Source_of_Funds_Concerns":  _yes_no(_to_bool(flags_json.get("source_of_funds_concerns"))),
        "Complex_Ownership":         _yes_no(_to_bool(flags_json.get("complex_ownership"))),
    }

def _extract_questionnaire_fields(q_json: dict) -> dict:
    if not isinstance(q_json, dict): q_json = {}
    nat = q_json.get("nationality")
    cor = q_json.get("country_of_residence")
    return {"Nationality": nat if isinstance(nat, str) else "", "Country_of_Residence": cor if isinstance(cor, str) else ""}

# BUILD KYC INPUT FROM DELTA (no CSV)
def fetch_kyc_input_from_delta(limit_rows: int = 1000) -> pd.DataFrame:
    a = CONFIG["kyc_table_a"]; b = CONFIG["kyc_table_b"]
    df_raw = sql_to_df(f"""
        SELECT
            A.case_id       AS a__case_id,
            A.customer_name AS a__customer_name,
            A.aml_json      AS a__aml_json,
            A.created_at    AS a__created_at,
            B.compliance_flags_json        AS b__compliance_flags_json,
            B.questionnaire_responses_json AS b__questionnaire_responses_json
        FROM {a} A
        LEFT JOIN {b} B
          ON A.case_id = B.case_id
        ORDER BY A.created_at DESC
        LIMIT {int(limit_rows)}
    """)
    out_rows = []
    for _, r in df_raw.iterrows():
        aml   = _ensure_json_obj(r.get("a__aml_json"))
        flags = _ensure_json_obj(r.get("b__compliance_flags_json"))
        ques  = _ensure_json_obj(r.get("b__questionnaire_responses_json"))

        aml_flags  = _extract_fields_from_aml(aml)
        comp_flags = _extract_compliance_flags(flags)
        q_fields   = _extract_questionnaire_fields(ques)

        out_rows.append({
            "Customer_ID": r.get("a__case_id"),
            "Customer_Name": aml_flags.get("Customer_Name") or r.get("a__customer_name"),
            "Sanctions_Hit": aml_flags.get("Sanctions_Hit"),
            "Internal_Watchlist": aml_flags.get("Internal_Watchlist"),
            "Is_PEP": aml_flags.get("Is_PEP"),
            "Interpol_Notices": aml_flags.get("Interpol_Notices"),
            "Adverse_News": aml_flags.get("Adverse_News"),
            "Connected_Persons": aml_flags.get("Connected_Persons"),
            "High_Risk_Jurisdiction": comp_flags["High_Risk_Jurisdiction"],
            "Watchlist_Hit": comp_flags["Watchlist_Hit"],
            "Source_of_Wealth_Concerns": comp_flags["Source_of_Wealth_Concerns"],
            "Source_of_Funds_Concerns": comp_flags["Source_of_Funds_Concerns"],
            "Complex_Ownership": comp_flags["Complex_Ownership"],
            "Nationality": q_fields["Nationality"],
        })
    cols = [
        "Customer_ID","Customer_Name",
        "Sanctions_Hit","Internal_Watchlist","Is_PEP",
        "Interpol_Notices","Adverse_News","Connected_Persons",
        "High_Risk_Jurisdiction","Watchlist_Hit",
        "Source_of_Wealth_Concerns","Source_of_Funds_Concerns","Complex_Ownership",
        "Nationality"
    ]
    return pd.DataFrame(out_rows, columns=cols)

# RULES LOADER (UC Volumes via Files API or local file)
EMBEDDED_RULES_YAML = """
# === Risk model tuned to your CSV ===
# Each boolean factor contributes (weight * 100) points when "Yes".
# 'overrides' lets certain red flags force HIGH/MEDIUM irrespective of the numeric score.
weights:
  # AML/Screening signals
  internal_watchlist: 0.35   # A hard red flag; also in overrides -> HIGH
  sanctions: 0.30            # Any external sanctions hit; overrides -> HIGH
  watchlist_hit: 0.20        # Compliance watchlist; overrides -> HIGH
  is_pep: 0.10               # PEP elevates risk; overrides floor -> MEDIUM
  interpol: 0.05             # Interpol indications
  adverse_media: 0.04        # Negative media
  # Jurisdiction / profile
  high_risk_jurisdiction: 0.01
  # KYC/compliance concerns
  source_of_wealth_concerns: 0.05
  source_of_funds_concerns: 0.10
  complex_ownership: 0.05
  # Nationality (optional; keep 0 or raise if you want impact)
  nationality: 0.00

# Score bands (numeric thresholds on the final weighted score)
bands:
  high: 70
  medium: 40

kyc:
  # For 'nationality' factor: if CSV Nationality is in this list and weight > 0, apply it.
  high_risk_countries: ["South Africa", "China"]
  # Normalize Yes/No coming from CSV
  yes_values: ["Yes", "Y", "True", "1"]
  no_values: ["No", "N", "False", "0"]

# Column mapping from your CSV -> rule factor names above.
# (Helps keep scoring code clean and resilient to header changes.)
columns:
  is_pep: "Is_PEP"
  sanctions: "Sanctions_Hit"
  internal_watchlist: "Internal_Watchlist"
  interpol: "Interpol_Notices"
  adverse_media: "Adverse_News"
  high_risk_jurisdiction: "High_Risk_Jurisdiction"
  watchlist_hit: "Watchlist_Hit"
  source_of_wealth_concerns: "Source_of_Wealth_Concerns"
  source_of_funds_concerns: "Source_of_Funds_Concerns"
  complex_ownership: "Complex_Ownership"
  nationality_value: "Nationality"
"""

# Drop-in replacement for load_rules() to support embedded rules
def load_rules(path: str) -> dict:
    """
    If path is '__EMBEDDED__' (or empty), load from the inlined YAML above.
    Otherwise behaves exactly like your current loader (Volumes/local).
    """
    # Use embedded rules if a sentinel is provided or path is empty
    if not path or path.strip().upper() == "__EMBEDDED__":
        return yaml.safe_load(EMBEDDED_RULES_YAML)
    else: 
        print("EMBEDDED RULES NOT FOUND ")

# RISK RULES + DETERMINISTIC SCORER
def map_resolution(risk_level: str) -> str:
    u = (risk_level or "").upper()
    if u == "HIGH": return "MGMT_APPROVAL"
    if u == "MEDIUM": return "CONT_MONITORING"
    return "APPROVE"

def yn_to_bool(v) -> bool:
    return str(v or "").strip().lower() in {"yes","y","true","1"}

def score_row_from_flags(row: dict, rules: dict) -> dict:
    flags = {
        "sanctions_hit": yn_to_bool(row.get("Sanctions_Hit")),
        "internal_watchlist": yn_to_bool(row.get("Internal_Watchlist")),
        "is_pep": yn_to_bool(row.get("Is_PEP")),
        "interpol_notices": yn_to_bool(row.get("Interpol_Notices")),
        "adverse_news": yn_to_bool(row.get("Adverse_News")),
        "watchlist_hit": yn_to_bool(row.get("Watchlist_Hit")),
        "high_risk_jurisdiction": yn_to_bool(row.get("High_Risk_Jurisdiction")),
        "source_of_wealth_concerns": yn_to_bool(row.get("Source_of_Wealth_Concerns")),
        "source_of_funds_concerns": yn_to_bool(row.get("Source_of_Funds_Concerns")),
        "complex_ownership": yn_to_bool(row.get("Complex_Ownership")),
    }
    nationality = (row.get("Nationality") or "").strip()

    weights = rules.get("weights", {})
    high_countries = set([str(x).strip().upper() for x in rules.get("kyc", {}).get("high_risk_countries", [])])

    def crit(id_key, hit: bool, rationale_true, rationale_false):
        score = 100 if hit else 0
        level = "high" if score == 100 else "low"
        rationale = rationale_true if hit else rationale_false
        return {"id": id_key, "level": level, "score": score, "rationale": rationale}

    criterion = []
    def add_if_weighted(id_key, fn):
        if id_key in weights: criterion.append(fn())

    add_if_weighted("is_pep",                  lambda: crit("is_pep", flags["is_pep"], "PEP flag true.", "PEP flag false."))
    add_if_weighted("sanctions",               lambda: crit("sanctions", flags["sanctions_hit"], "Sanctions hit true.", "Sanctions hit false."))
    add_if_weighted("internal_watchlist",      lambda: crit("internal_watchlist", flags["internal_watchlist"], "Internal watchlist true.", "Internal watchlist false."))
    add_if_weighted("watchlist_hit",           lambda: crit("watchlist_hit", flags["watchlist_hit"], "Compliance watchlist true.", "Compliance watchlist false."))
    add_if_weighted("interpol",                lambda: crit("interpol", flags["interpol_notices"], "Interpol notices true.", "Interpol notices false."))
    add_if_weighted("adverse_media",           lambda: crit("adverse_media", flags["adverse_news"], "Adverse media true.", "Adverse media false."))
    add_if_weighted("high_risk_jurisdiction",  lambda: crit("high_risk_jurisdiction", flags["high_risk_jurisdiction"], "High-risk jurisdiction true.", "High-risk jurisdiction false."))
    add_if_weighted("source_of_wealth_concerns", lambda: crit("source_of_wealth_concerns", flags["source_of_wealth_concerns"], "Source of wealth concerns true.", "Source of wealth concerns false."))
    add_if_weighted("source_of_funds_concerns", lambda: crit("source_of_funds_concerns", flags["source_of_funds_concerns"], "Source of funds concerns true.", "Source of funds concerns false."))
    add_if_weighted("complex_ownership",       lambda: crit("complex_ownership", flags["complex_ownership"], "Complex ownership true.", "Complex ownership false."))

    nat_hit = nationality.strip().upper() in high_countries if nationality else False
    if "nationality" in weights:
        criterion.append(crit("nationality", nat_hit,
                              f"{nationality or 'N/A'} in high-risk list.",
                              f"{nationality or 'N/A'} not in high-risk list."))

    total = 0.0
    for c in criterion:
        w_ = float(weights.get(c["id"], 0.0))
        s_ = float(min(100, max(0, c["score"])))
        total += w_ * s_
    total = round(total, 2)

    bands = rules.get("bands", {})
    rating = "low"
    if {"low","medium","high"} <= set(bands.keys()):
        L0,L1 = bands["low"]; M0,M1 = bands["medium"]; H0,H1 = bands["high"]
        if H0 <= total <= H1: rating = "high"
        elif M0 <= total <= M1: rating = "medium"
        else: rating = "low"
    else:
        M = float(bands.get("medium", 50)); H = float(bands.get("high", 80))
        rating = "high" if total >= H else ("medium" if total >= M else "low")

    overrides = rules.get("overrides", {})
    high_if_true = set(overrides.get("high_if_true", []))
    med_floor    = set(overrides.get("medium_floor_if_true", []))

    def is_true(f):
        return (
            (f == "is_pep" and flags["is_pep"]) or
            (f == "sanctions" and flags["sanctions_hit"]) or
            (f == "internal_watchlist" and flags["internal_watchlist"]) or
            (f == "watchlist_hit" and flags["watchlist_hit"]) or
            (f == "interpol" and flags["interpol_notices"]) or
            (f == "adverse_media" and flags["adverse_news"]) or
            (f == "high_risk_jurisdiction" and flags["high_risk_jurisdiction"]) or
            (f == "source_of_wealth_concerns" and flags["source_of_wealth_concerns"]) or
            (f == "source_of_funds_concerns" and flags["source_of_funds_concerns"]) or
            (f == "complex_ownership" and flags["complex_ownership"]) or
            (f == "nationality" and nat_hit)
        )

    if any(is_true(f) for f in high_if_true):
        rating = "high"
    elif rating == "low" and any(is_true(f) for f in med_floor):
        rating = "medium"

    resolution = map_resolution(rating.upper())

    # Compact narrative
    lines = []
    lines.append("Overall risk is HIGH due to elevated indicators." if rating=="high"
                 else "Overall risk is MEDIUM based on notable indicators." if rating=="medium"
                 else "Overall risk is LOW with limited adverse indications.")
    lines.append("The customer is identified as a Politically Exposed Person (PEP)." if flags["is_pep"] else "No PEP designation is indicated.")
    if nationality: lines.append(f"Nationality on record: {nationality}.")
    hits = []
    if flags["sanctions_hit"]: hits.append("sanctions")
    if flags["internal_watchlist"]: hits.append("internal watchlist")
    if flags["watchlist_hit"]: hits.append("compliance watchlist")
    if flags["interpol_notices"]: hits.append("Interpol notices")
    if flags["adverse_news"]: hits.append("adverse media")
    lines.append(("Screening indicates the following hits: " + ", ".join(hits) + ".") if hits
                 else "Screening shows no sanctions, watchlist, Interpol, or adverse media hits.")
    concerns = []
    if flags["high_risk_jurisdiction"]: concerns.append("high-risk jurisdiction")
    if flags["source_of_wealth_concerns"]: concerns.append("source of wealth concerns")
    if flags["source_of_funds_concerns"]: concerns.append("source of funds concerns")
    if flags["complex_ownership"]: concerns.append("complex ownership")
    lines.append(("Additional KYC/AML concerns noted: " + ", ".join(concerns) + ".") if concerns else "No additional KYC/AML concerns noted.")
    lines.append("Enhanced due diligence and management approval are required." if rating=="high"
                 else "Ongoing monitoring is recommended." if rating=="medium"
                 else "Proceed with standard monitoring.")
    assessment = " ".join(lines[:6])

    susp_any = any([
        flags["sanctions_hit"], flags["internal_watchlist"], flags["interpol_notices"],
        flags["adverse_news"], flags["watchlist_hit"],
        flags["source_of_wealth_concerns"], flags["source_of_funds_concerns"], flags["complex_ownership"]
    ])
    evidence = {
        "pep_flag": bool(flags["is_pep"]),
        "suspicious_activity_flag": bool(susp_any),
        "nationality": nationality or "",
        "last_txn_country": "",
        "id_days_to_expiry": None,
        "account_type": "",
        "balance": None,
        "last_txn_amount": None,
        "ratio_last_txn_to_balance": None,
        "opening_date": None,
        "sanctions_hit": bool(flags["sanctions_hit"]),
        "internal_watchlist": bool(flags["internal_watchlist"]),
        "interpol_notices": bool(flags["interpol_notices"]),
        "adverse_media": bool(flags["adverse_news"]),
        "watchlist_hit": bool(flags["watchlist_hit"]),
        "high_risk_jurisdiction": bool(flags["high_risk_jurisdiction"]),
        "source_of_wealth_concerns": bool(flags["source_of_wealth_concerns"]),
        "source_of_funds_concerns": bool(flags["source_of_funds_concerns"]),
        "complex_ownership": bool(flags["complex_ownership"]),
    }

    return {
        "customer_id": str(row.get("Customer_ID") or "").strip(),
        "full_name": str(row.get("Customer_Name") or "").strip(),
        "risk_level": rating.upper(),
        "score": total,
        "resolution": resolution,
        "assessment": assessment,
        "evidence": evidence
    }

def score_batch(df: pd.DataFrame, rules: dict) -> list[dict]:
    return [score_row_from_flags(dict(r), rules) for _, r in df.iterrows()]

# OPTIONAL: LLM batch writer (JSON-only narratives)
def compose_writeup_batch_llama(reports: list[dict]) -> list[dict]:
    SYSTEM_PROMPT = """
You are a KYC Risk Narrative Agent for Compliance.
Return STRICT JSON only. No markdown.
If input contains "batch", return: { "reports": [ { ... }, ... ] }

Keys per item:
 customer_id, full_name, risk_level, score, assessment, resolution, sow_conclusion, evidence

Rules:
- Echo customer_id, full_name, risk_level, score exactly as given.
- If resolution missing, derive: high→MGMT_APPROVAL, medium→CONT_MONITORING, low→APPROVE.
- Use only provided fields and evidence; no invention.
- Assessment: 3–7 concise factual sentences.
""".strip()

    payload = {"batch": [
        {
            "customer_id": r["customer_id"],
            "full_name": r.get("full_name",""),
            "risk_level": r["risk_level"],
            "score": r["score"],
            "resolution": r["resolution"],
            "evidence": r.get("evidence", {})
        } for r in reports
    ]}
    raw = call_model(json.dumps({"system": SYSTEM_PROMPT, "input": payload}))
    try:
        obj = safe_json_from_llm(raw)
        if isinstance(obj, dict) and isinstance(obj.get("reports"), list):
            return obj["reports"]
    except Exception:
        pass

    # Fallback
    out = []
    for r in reports:
        out.append({
            "customer_id": r["customer_id"],
            "full_name": r.get("full_name",""),
            "risk_level": r["risk_level"],
            "score": r["score"],
            "assessment": r.get("assessment","Fallback: narrative unavailable"),
            "resolution": r["resolution"],
            "sow_conclusion": "Not applicable",
            "evidence": r.get("evidence", {})
        })
    return out

# SAVE RISK RESULTS TO DELTA (multi-row insert)
def upsert_risk_results_to_delta(results: list[dict]):
    """
    Upserts into dbx_main.ca_bronze.indentified_risk_output by case_id.
    - Normalizes case_id (trim + lower) in source to ensure ON condition matches.
    - When matched: update fields + set updated_at = current_timestamp().
    - When not matched: insert row + set created_at/updated_at = current_timestamp().
    """
    table = "dbx_main.ca_bronze.indentified_risk_output"
    batch_session_id = str(uuid.uuid4())  # one session id per batch write

    # Build USING block with normalized case_id
    rows_select = []
    for r in results:
        # Normalize the case_id *in the source* so it matches the normalized table values
        cid_norm = escape_sql(r["customer_id"]).strip().lower()
        rows_select.append(
            "SELECT "
            f"'{cid_norm}' AS case_id, "
            f"'{escape_sql(r.get('full_name', ''))}' AS customer_name, "
            f"'{escape_sql(r['risk_level'])}' AS Identified_Risk_Level, "
            f"{float(r['score'])} AS Score, "
            f"'{escape_sql(r.get('assessment', ''))}' AS Assessment, "
            f"'{escape_sql(r['resolution'])}' AS Resolution, "
            f"'{escape_sql(batch_session_id)}' AS session_id"
        )

    using_block = "\nUNION ALL\n".join(rows_select)

    # MERGE with normalized ON condition
    merge_sql = f"""
    MERGE INTO {table} AS t
    USING (
      {using_block}
    ) s
    ON TRIM(LOWER(t.case_id)) = s.case_id
    WHEN MATCHED THEN UPDATE SET
      t.customer_name          = s.customer_name,
      t.Identified_Risk_Level  = s.Identified_Risk_Level,
      t.Score                  = s.Score,
      t.Assessment             = s.Assessment,
      t.Resolution             = s.Resolution,
      t.session_id             = s.session_id,
      t.updated_at             = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
      case_id, customer_name, Identified_Risk_Level, Score, Assessment, Resolution,
      session_id, created_at, updated_at
    ) VALUES (
      s.case_id, s.customer_name, s.Identified_Risk_Level, s.Score, s.Assessment, s.Resolution,
      s.session_id, current_timestamp(), current_timestamp()
    );
    """

    sql_execute(merge_sql)

# UI NAVIGATION
st.sidebar.title("🗂️ Navigation")
page = st.sidebar.radio("Go to:", [
    "1️⃣ Preliminary AML Screening",
    "2️⃣ ECDD Screening (Source of Wealth)",
    "3️⃣ Risk Assessment (Rules + Narrative)",
    "4️⃣ KYC Compliance Reports"
])
render_activity_log()  # Show CA progress tracker in sidebar
st.title("🛡️ Onboarding AI")

# PAGE 1 – AML SCREENING
if page == "1️⃣ Preliminary AML Screening":

    st.header("🔍 Preliminary AML Screening")
    
    # Show current status and activity log
    render_activity_log()

    uploaded = st.file_uploader(
        "📄 Upload onboarding document (PDF)", type="pdf")

    if uploaded and st.button("🚀 Run AML Screening"):
        case_id = str(uuid.uuid4())
        clear_activity_log()
        
        # Single consolidated status block for better performance
        with LiveStatus("🔍 AML Screening in Progress", "AML Screening") as status:
            # Document Analysis
            status.log("Reading and analyzing uploaded document...", "Document Analysis")
            text = extract_text_from_pdf(uploaded.read())
            doc_type = detect_document_type(text)
            status.log(f"Document classified as: {doc_type}")
            fields = extract_fields(text, doc_type)
            save_to_delta(fields, doc_type, case_id)
            name = fetch_name(doc_type, case_id)
            status.log(f"Customer identified: {name}", "AML Checks in Progress")
            
            # AML Checks - consolidated logging
            status.log("Checking global sanctions databases...")
            opensanctions_result = tool_opensanctions(name)
            un_result = tool_un(name)
            ofac_result = tool_ofac(name)
            eu_result = tool_eu(name)
            status.log("Checking enforcement databases...")
            interpol_result = tool_interpol(name)
            adverse_result = tool_adverse_news(name)
            watchlist_result = tool_internal_watchlist(name)
            
            # Report results
            hits = sum([1 for r in [opensanctions_result, un_result, ofac_result, eu_result, interpol_result, adverse_result] if r])
            status.log(f"Screening complete: {hits} database(s) with potential matches")
            
            # Generate Report
            status.log("Generating risk assessment report...")
            aml_report = agentic_aml_screen(name)
            risk = aml_report.get("risk_score", "REVIEW")
            save_aml_report(case_id=case_id, customer_name=name, document_type=doc_type, aml_report=aml_report)
            status.log(f"✓ AML Complete - Risk: {risk}", f"AML Complete ({risk}) → Ready for ECDD")
            notify_module_complete("AML Screening", case_id, name, f"Completed - Risk: {risk}")

        st.success(f"✅ AML Screening Completed for {name}")
        st.json(aml_report)

# -----------------------------------
# PAGE 2 — ECDD SCREENING (Enhanced)
# -----------------------------------
if page == "2️⃣ ECDD Screening (Source of Wealth)":

    st.header("🧾 ECDD – Enhanced Customer Due Diligence")
    
    # Show current status and activity log
    render_activity_log()

    # Initialize session state for ECDD
    if "ecdd_questionnaire" not in st.session_state:
        st.session_state.ecdd_questionnaire = None
    if "ecdd_assessment" not in st.session_state:
        st.session_state.ecdd_assessment = None
    if "ecdd_checklist" not in st.session_state:
        st.session_state.ecdd_checklist = None
    if "ecdd_responses" not in st.session_state:
        st.session_state.ecdd_responses = {}
    if "ecdd_selected_case" not in st.session_state:
        st.session_state.ecdd_selected_case = None
    if "ecdd_pdf_bytes" not in st.session_state:
        st.session_state.ecdd_pdf_bytes = None
    if "ecdd_session_id" not in st.session_state:
        st.session_state.ecdd_session_id = None

    # Load AML cases
    try:
        df = load_aml_cases()

        if df.empty:
            st.warning("No AML cases found. Please run AML screening first.")
            st.stop()

        # Case selector
        case = st.selectbox(
            "📋 Select AML Case for ECDD",
            options=df["case_id"].tolist(),
            format_func=lambda cid: f"{cid[:8]}... — {df[df['case_id'] == cid]['customer_name'].iloc[0]} ({df[df['case_id'] == cid]['risk_score'].iloc[0]})"
        )

        row = df[df["case_id"] == case].iloc[0]
        customer_name = row["customer_name"]
        aml_json = row["aml_json"]
        risk_score = row["risk_score"]

        # Show client info card
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"""
            <div class="kpi">
                <div style="font-size:12px;color:#9ca3af">Customer</div>
                <div style="font-size:18px;font-weight:600">{customer_name}</div>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            risk_class = risk_score.lower() if risk_score else "medium"
            st.markdown(f"""
            <div class="kpi risk-{risk_class}">
                <div style="font-size:12px;color:#9ca3af">AML Risk Score</div>
                <div class="{risk_class}" style="font-size:24px">{risk_score or 'REVIEW'}</div>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div class="kpi">
                <div style="font-size:12px;color:#9ca3af">Case ID</div>
                <div style="font-size:14px">{case[:12]}...</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("---")

        # Auto-generate questionnaire if case changed
        if st.session_state.ecdd_selected_case != case:
            st.session_state.ecdd_selected_case = case
            st.session_state.ecdd_questionnaire = None
            st.session_state.ecdd_assessment = None
            st.session_state.ecdd_checklist = None
            st.session_state.ecdd_responses = {}
            st.session_state.ecdd_pdf_bytes = None
            st.session_state.ecdd_session_id = None

            with LiveStatus(f"📝 Generating ECDD Questionnaire", "ECDD") as status:
                status.log(f"Analyzing {customer_name}'s profile...", "ECDD in Progress")
                st.session_state.ecdd_questionnaire = generate_ecdd_questionnaire(
                    aml_json, customer_name, case)
                num_q = len(st.session_state.ecdd_questionnaire.all_questions())
                status.log(f"✓ Generated {num_q} personalized questions", "ECDD Questionnaire Ready")
            st.success("Questionnaire generated!")
            st.rerun()

        # TABS for ECDD workflow
        tab1, tab2, tab3 = st.tabs(
            ["📝 Questionnaire", "📊 Assessment Report", "📂 Document Checklist"])

        # ==========================================
        # TAB 1: DYNAMIC QUESTIONNAIRE
        # ==========================================
        with tab1:
            if st.session_state.ecdd_questionnaire:
                q = st.session_state.ecdd_questionnaire

                st.markdown(f"**Inferred Client Type:** {q.client_type}")
                st.markdown("---")

                # Collect responses by section
                responses = {}

                for section in q.sections:
                    st.subheader(
                        f"{section.section_icon} {section.section_title}")
                    if section.section_description:
                        st.caption(section.section_description)

                    for question in section.questions:
                        field_id = question.field_id
                        q_text = question.question_text
                        q_type = question.question_type
                        required = question.required
                        help_text = question.help_text
                        options = question.options

                        label = f"{q_text} {'*' if required else ''}"

                        # Render based on question type
                        if q_type == "yes_no":
                            answer = st.radio(
                                label, ["Yes", "No", "N/A"], key=f"q_{field_id}", horizontal=True, help=help_text)
                        elif q_type == "dropdown" and options:
                            answer = st.selectbox(
                                label, ["-- Select --"] + options, key=f"q_{field_id}", help=help_text)
                            if answer == "-- Select --":
                                answer = ""
                        elif q_type == "multiple_choice" and options:
                            answer = st.multiselect(
                                label, options, key=f"q_{field_id}", help=help_text)
                            answer = ", ".join(answer) if answer else ""
                        elif q_type == "checkbox":
                            answer = "Yes" if st.checkbox(
                                label, key=f"q_{field_id}", help=help_text) else "No"
                        elif q_type == "date":
                            date_val = st.date_input(
                                label, key=f"q_{field_id}", help=help_text, value=None)
                            answer = str(date_val) if date_val else ""
                        elif q_type == "number" or q_type == "currency":
                            answer = st.number_input(
                                label, key=f"q_{field_id}", help=help_text, value=0, step=1)
                            answer = str(answer) if answer else ""
                        elif q_type == "textarea":
                            answer = st.text_area(
                                label, key=f"q_{field_id}", height=100, help=help_text)
                        else:  # text
                            answer = st.text_input(
                                label, key=f"q_{field_id}", help=help_text)

                        responses[field_id] = answer

                    st.markdown("---")

                # Store responses in session state
                st.session_state.ecdd_responses = responses

                # Generate Assessment button
                if st.button("📊 Generate ECDD Assessment", use_container_width=True, type="primary"):
                    with LiveStatus(f"📊 Generating ECDD Assessment", "ECDD") as status:
                        status.log("Analyzing responses and generating assessment...", "ECDD Assessment in Progress")
                        assessment, checklist = generate_ecdd_assessment(
                            customer_name, aml_json, responses, case
                        )
                        st.session_state.ecdd_assessment = assessment
                        st.session_state.ecdd_checklist = checklist
                        level = assessment.overall_ecdd_level
                        status.log(f"ECDD Level: {level}")
                        session_id = save_ecdd_to_delta(case, customer_name, assessment, checklist, responses)
                        if session_id:
                            st.session_state.ecdd_session_id = session_id
                            status.log(f"✓ Assessment saved", f"ECDD Complete ({level}) → Risk Assessment Ready")
                            notify_module_complete("ECDD Assessment", case, customer_name, f"Level: {level}")
                            st.success(f"✅ Assessment saved! Session: {session_id[:8]}...")
                        else:
                            st.warning("⚠️ Assessment generated but save failed.")

                    st.rerun()
            else:
                if st.button("🔄 Generate Questionnaire"):
                    with st.spinner("Generating dynamic questionnaire..."):
                        st.session_state.ecdd_questionnaire = generate_ecdd_questionnaire(
                            aml_json, customer_name, case)
                    st.rerun()

        # ==========================================
        # TAB 2: ASSESSMENT REPORT
        # ==========================================
        with tab2:
            if st.session_state.ecdd_assessment:
                assessment = st.session_state.ecdd_assessment

                # ECDD Level badge
                level_colors = {"LOW": "#22c55e",
                                "MEDIUM": "#facc15", "HIGH": "#ef4444"}
                level_color = level_colors.get(
                    assessment.overall_ecdd_level.upper(), "#666")

                st.markdown(f"""
                <div style="text-align:center;padding:20px">
                    <h3>ECDD Assessment Level</h3>
                    <div style="display:inline-block;background:{level_color};color:white;
                                font-size:24px;padding:12px 40px;border-radius:8px;font-weight:bold">
                        {assessment.overall_ecdd_level.upper()}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                st.markdown(f"**Client Type:** {assessment.client_type}")
                st.markdown(
                    f"**Client Category:** {assessment.client_category}")
                st.markdown("---")

                # Source of Wealth
                st.subheader("💰 Source of Wealth")
                sow_text = assessment.source_of_wealth.get("summary", "") if isinstance(
                    assessment.source_of_wealth, dict) else str(assessment.source_of_wealth)
                st.write(sow_text or "No information provided.")

                # Source of Funds
                st.subheader("💵 Source of Funds")
                sof_text = assessment.source_of_funds.get("summary", "") if isinstance(
                    assessment.source_of_funds, dict) else str(assessment.source_of_funds)
                st.write(sof_text or "No information provided.")

                # Compliance Flags
                st.subheader("🚩 Compliance Flags")
                cols = st.columns(4)
                flags = assessment.compliance_flags
                flag_items = [
                    ("PEP", flags.pep), ("Sanctions", flags.sanctions),
                    ("Adverse Media", flags.adverse_media), ("High-Risk Jurisdiction",
                                                             flags.high_risk_jurisdiction)
                ]
                for i, (name, value) in enumerate(flag_items):
                    with cols[i]:
                        icon = "⚠️" if value else "✅"
                        color = "red" if value else "green"
                        st.markdown(
                            f"<span style='color:{color}'>{icon} {name}</span>", unsafe_allow_html=True)

                # More flags row
                cols2 = st.columns(4)
                flag_items2 = [
                    ("Watchlist Hit", flags.watchlist_hit), ("SOW Concerns",
                                                             flags.source_of_wealth_concerns),
                    ("SOF Concerns", flags.source_of_funds_concerns), (
                        "Complex Ownership", flags.complex_ownership)
                ]
                for i, (name, value) in enumerate(flag_items2):
                    with cols2[i]:
                        icon = "⚠️" if value else "✅"
                        color = "red" if value else "green"
                        st.markdown(
                            f"<span style='color:{color}'>{icon} {name}</span>", unsafe_allow_html=True)

                # Recommendations
                if assessment.recommendations:
                    st.subheader("📋 Recommendations")
                    for i, rec in enumerate(assessment.recommendations, 1):
                        st.markdown(f"{i}. {rec}")

                # Required Actions
                if assessment.required_actions:
                    st.subheader("⚡ Required Actions")
                    for i, action in enumerate(assessment.required_actions, 1):
                        st.markdown(f"{i}. {action}")

                # RM Guidance
                if assessment.rm_guidance:
                    with st.expander("🧭 Guidance for Relationship Manager"):
                        st.write(assessment.rm_guidance)

                # Full report text
                if assessment.report_text:
                    with st.expander("📄 Full Report Text"):
                        st.write(assessment.report_text)
            else:
                st.info(
                    "Complete the questionnaire and generate an assessment to view the report.")

        # ==========================================
        # TAB 3: DOCUMENT CHECKLIST
        # ==========================================
        with tab3:
            if st.session_state.ecdd_checklist:
                checklist = st.session_state.ecdd_checklist

                st.subheader("📂 Required Documents")
                st.caption(
                    "Documents needed to complete the ECDD verification process.")

                categories = [
                    ("🪪 Identity Documents", checklist.identity_documents),
                    ("💰 Source of Wealth Documents",
                     checklist.source_of_wealth_documents),
                    ("💵 Source of Funds Documents",
                     checklist.source_of_funds_documents),
                    ("📋 Compliance Documents", checklist.compliance_documents),
                    ("📎 Additional Documents", checklist.additional_documents),
                ]

                for cat_name, docs in categories:
                    if docs:
                        st.markdown(f"**{cat_name}**")
                        for doc_item in docs:
                            priority_badge = {
                                "required": "🔴 Required",
                                "recommended": "🟡 Recommended",
                                "optional": "🟢 Optional"
                            }.get(doc_item.priority.lower(), doc_item.priority)

                            collected = st.checkbox(
                                f"{doc_item.document_name} — {priority_badge}",
                                key=f"doc_{hash(doc_item.document_name)}"
                            )
                            if doc_item.special_instructions:
                                st.caption(
                                    f"  ℹ️ {doc_item.special_instructions}")
                        st.markdown("---")

                # Summary
                total_docs = len(checklist.all_documents())
                required_docs = len(
                    [d for d in checklist.all_documents() if d.priority.lower() == "required"])
                st.metric("Total Documents", total_docs,
                          delta=f"{required_docs} required")
            else:
                st.info("Generate an assessment to view the document checklist.")

    except Exception as e:
        st.error(f"Error loading AML cases: {e}")
        st.info("Make sure you have run AML screening first to create cases.")

# PAGE 3 – RISK ASSESSMENT (Rules + optional LLM narrative)
if page == "3️⃣ Risk Assessment (Rules + Narrative)":
    st.header("🧮 Risk Assessment (Rules) → 📝 Narrative")
    
    # Show current status and activity log
    render_activity_log()

    # --- Controls ---
    left, right = st.columns([1, 1])

    with left:
        use_llm = st.checkbox("🧠 Generate LLM narrative", value=True)

    # Load candidates from Delta
    candidates_df = fetch_kyc_input_from_delta()
    if candidates_df.empty:
        st.warning("No candidates found. Run AML + ECDD first.")
        st.stop()

    # Case selector - multi-select for batch scoring
    with left:
        options = candidates_df["Customer_ID"].tolist()
        labels = {cid: f"{candidates_df[candidates_df['Customer_ID']==cid]['Full_Name'].iloc[0]} ({cid[:8]}...)" for cid in options}
        selected_labels = st.multiselect(
            "👤 Select Cases to Score",
            options=[labels[cid] for cid in options],
            default=[],
        )
        label_to_id = {labels[cid]: cid for cid in options}
        selected_case_ids = [label_to_id[lbl] for lbl in selected_labels]

    with right:
        st.write("**Output table**:", CONFIG["risk_output_table"])

    # --- Initialize session state holders (persist across reruns) ---
    if "risk_reports" not in st.session_state:
        st.session_state.risk_reports = None
    if "risk_preview_df" not in st.session_state:
        st.session_state.risk_preview_df = None

    # --- RUN SCORING ---
    if st.button("🚀 Run Risk Scoring"):
        if not selected_case_ids:
            st.error("Please select at least one person/case to score.")
        else:
            try:
                with LiveStatus(f"🧮 Scoring {len(selected_case_ids)} Case(s)", "Risk Assessment") as status:
                    status.log("Loading rules and preparing data...", "Risk Assessment in Progress")
                    rules = load_rules(CONFIG["risk_rules_path"])
                    df_in = candidates_df[candidates_df["Customer_ID"].isin(selected_case_ids)]
                    if df_in.empty:
                        st.error("Selected cases not found.")
                        st.stop()
                    status.log(f"Scoring {len(df_in)} candidate(s)...")
                    base_reports = score_batch(df_in, rules)
                    status.log(f"✓ Scoring complete")

                final_reports = base_reports
                if use_llm:
                    with LiveStatus("📝 Generating Narratives", "Risk Assessment") as status:
                        status.log("Generating compliance narratives...")
                        final_reports = compose_writeup_batch_llama(base_reports)
                        status.log("✓ Narratives complete", "Risk Scoring Complete")

                preview_cols = ["customer_id", "full_name", "risk_level", "score", "assessment", "resolution"]
                st.session_state.risk_reports = final_reports
                st.session_state.risk_preview_df = pd.DataFrame(
                    [{k: r.get(k) for k in preview_cols} for r in final_reports]
                )
                st.success(f"✅ Scoring complete for {len(final_reports)} record(s).")

            except FileNotFoundError as e:
                st.error(f"rules.yml not found or not readable. Fix CONFIG['risk_rules_path'] or permissions.\n\nDetails: {e}")
            except Exception as e:
                st.error(f"Risk scoring failed: {e}")

    # --- PREVIEW + SAVE (persists across reruns) ---
    reports = st.session_state.get("risk_reports")
    if reports:
        st.write("### Preview")
        st.dataframe(st.session_state.risk_preview_df.head(20))

        if st.button("💾 Save results to Delta"):
            reports = st.session_state.get("risk_reports")
            if not reports:
                st.info("Run risk scoring to generate results before saving.")
            else:
                try:
                    with LiveStatus("💾 Saving Risk Assessment Results", "Risk Assessment") as status:
                        status.log(f"Preparing {len(reports)} record(s) for database...")
                        status.log("Upserting into Delta table...")
                        upsert_risk_results_to_delta(reports)
                        status.log("✓ All records saved successfully")
                        status.log("Sending CA notifications...", "Risk Assessment Complete → Review Required")
                        for r in reports:
                            notify_module_complete("Risk Assessment", r.get("customer_id", ""), r.get("full_name", ""), f"Level: {r.get('risk_level', 'N/A')}")
                    st.success(f"✅ Saved {len(reports)} row(s) to Delta successfully!")
                except Exception as e:
                    st.error("Save failed.")
                    st.exception(e)

    else:
        st.info("Select cases and run risk scoring to see results.")
#END OF RISK ASSESSMENT MODULE
#kyc_module_complines report
# ===== Tools =====
@tool
def trigger_workflow(job_id: int) -> dict:
    """Triggers a Databricks Job workflow"""
    headers = {"Authorization": f"Bearer {CONFIG['DATABRICKS_TOKEN']}", "Content-Type": "application/json"}
    payload = {"job_id": job_id, "notebook_params": {"execution_id": str(int(time.time()))}}
    resp = requests.post(f"{CONFIG['DATABRICKS_URL']}/api/2.1/jobs/run-now", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()
 
@tool
def poll_workflow(run_id: int) -> dict:
    """Polls a Databricks Job run until completion"""
    headers = {"Authorization": f"Bearer {CONFIG['DATABRICKS_TOKEN']}"}
    while True:
        resp = requests.get(f"{CONFIG['DATABRICKS_URL']}/api/2.1/jobs/runs/get", headers=headers, params={"run_id": run_id})
        data = resp.json()
        state = data["state"]["life_cycle_state"]
        if state in ("TERMINATED", "INTERNAL_ERROR"):
            return data
        time.sleep(10)
 
@tool
def get_task_runs(parent_run_data: dict):
    """Returns all task runs for a parent workflow"""
    return parent_run_data.get("tasks", [])
 
@tool
def get_task_output(run_id: int) -> dict:
    """Gets Databricks task output"""
    headers = {"Authorization": f"Bearer {CONFIG['DATABRICKS_TOKEN']}"}
    resp = requests.get(f"{CONFIG['DATABRICKS_URL']}/api/2.1/jobs/runs/get-output", headers=headers, params={"run_id": run_id})
    return resp.json()
 
@tool
def generate_final_report(task_outputs: list) -> str:
    """Generates a consolidated report from multiple task outputs"""
    combined = {"tasks": task_outputs, "summary": "Compliance workflow completed"}
    return json.dumps(combined, indent=2)
 
 
# ===== Agents =====
# Planner Agent – Decides which agents to call
planner_llm = AzureChatOpenAI(
    deployment_name="gpt-4o",
    azure_endpoint=CONFIG["AZURE_OPENAI_ENDPOINT"],
    api_key=CONFIG["AZURE_OPENAI_API_KEY"],
    api_version=CONFIG["AZURE_OPENAI_API_VERSION"],
    temperature=0
)
 
# Bind tools to separate agents
workflow_agent = planner_llm.bind_tools([trigger_workflow, poll_workflow])
results_agent = planner_llm.bind_tools([get_task_runs, get_task_output])
reporting_agent = planner_llm.bind_tools([generate_final_report])
 
# ===== Orchestration Logic =====
user_task = "Run the Databricks compliance workflow and produce a full compliance report."
 
# Step 1 — Planner decides
plan_response = planner_llm.invoke([HumanMessage(content=f"Plan steps for: {user_task}")])
print("📝 Planner plan:", plan_response.content)
 
## Step 2 — Ask the LLM to plan the action
wf_response = workflow_agent.invoke([
    HumanMessage(content=f"Please use the trigger_workflow tool with job_id={934858689153610}")
])
 
# Make sure we *got a* tool call back
if not getattr(wf_response, "tool_calls", None):
    raise Exception("❌ No tool call from the LLM to run workflow.")
 
# Get the first tool call
tool_call = wf_response.tool_calls[0]
tool_name = tool_call["name"]
tool_args = tool_call.get("args", {})
 
print(f"🤖 LLM decided to use tool: {tool_name} with args {tool_args}")
 
# Actually execute the matching tool
if tool_name == "trigger_workflow":
    tool_result_dict = trigger_workflow.invoke(tool_args)
else:
    raise Exception(f"❌ Unknown tool called by LLM: {tool_name}")
 
# Now get the run_id from the *actual function return*
run_id = tool_result_dict["run_id"]
print(f"✅ Workflow triggered | Run ID = {run_id}")
 
 
# Step 3 — Poll workflow
parent_data = poll_workflow.invoke({"run_id": run_id})
 
# Step 4 — Get task runs
task_runs = get_task_runs.invoke({"parent_run_data": parent_data})
 
# Step 5 — Get outputs for all tasks
task_outputs = []
for task in task_runs:
    output = get_task_output.invoke({"run_id": task["run_id"]})
    task_outputs.append(output)
 
# Step 6 — Generate final report
final_report = generate_final_report.invoke({"task_outputs": task_outputs})
 
print("✅ Final Report:\n", final_report)

# KYC REPORTS (PDF DOWNLOAD)
if page == "4️⃣ KYC Compliance Reports":

    import os
    import requests
    from datetime import datetime
    import streamlit as st

    st.header("📄 KYC Compliance Reports")
    st.caption("Authorized download of generated KYC compliance PDF reports")
    st.divider()

    DATABRICKS_HOST = CONFIG["DATABRICKS_URL"].rstrip("/")
    DATABRICKS_TOKEN = CONFIG["DATABRICKS_TOKEN"]

    REPORT_VOLUME_PATH = "/Volumes/dbx_main/ca_bronze/kycreports"

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    list_resp = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/fs/list",
        headers=headers,
        params={"path": REPORT_VOLUME_PATH}
    )

    if list_resp.status_code != 200:
        st.error("❌ Unable to access KYC report repository")
        st.code(list_resp.text)
        st.stop()

    files = list_resp.json().get("files", [])
    pdf_files = [f for f in files if f.get("path", "").lower().endswith(".pdf")]

    if not pdf_files:
        st.info("ℹ️ No KYC reports available")
        st.stop()

    pdf_files.sort(key=lambda x: x.get("modification_time", 0), reverse=True)

    col1, col2, col3, col4 = st.columns([5, 2, 2, 2])
    col1.markdown("**Report Name**")
    col2.markdown("**Generated On**")
    col3.markdown("**Size**")
    col4.markdown("**Action**")

    st.markdown("<hr>", unsafe_allow_html=True)

    for f in pdf_files:
        file_path = f["path"]
        file_name = os.path.basename(file_path)
        size_kb = round(f.get("file_size", 0) / 1024, 2)

        ts = f.get("modification_time", 0) / 1000
        generated_on = datetime.fromtimestamp(ts).strftime("%d %b %Y, %H:%M")

        pdf_resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.0/fs/files{file_path}",
            headers=headers,
            stream=True
        )

        c1, c2, c3, c4 = st.columns([5, 2, 2, 2])

        c1.markdown(f"📄 {file_name}")
        c2.markdown(generated_on)
        c3.markdown(f"{size_kb} KB")

        if pdf_resp.status_code != 200:
            c4.error("❌ Fetch failed")
            continue

        c4.download_button(
            label="Download",
            data=pdf_resp.content, # ✅ binary PDF bytes
            file_name=file_name,
            mime="application/pdf", # ✅ critical
            use_container_width=True
        )
