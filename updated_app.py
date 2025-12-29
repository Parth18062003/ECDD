# ============================================================
# IMPORTS
# ============================================================
from typing import Dict, Any, List, Optional
import io
import streamlit as st
import requests
import json
import re
import uuid
import os
import pandas as pd
from datetime import datetime, timezone
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential

# ============================================================
# PAGE CONFIG + DARK UI
# ============================================================
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

# ============================================================
# CONFIG
# ============================================================
CONFIG = {


    # PREVIOUS SINGLE TABLE REMOVED, NEW SEPARATE TABLES ADDED ↓
    "delta_tables": {
        "Bank Customer Form": "dbx_main.ca_bronze.customer_forms",
        "Bank Statement": "dbx_main.ca_bronze.bank_statements",
        "Self Declaration Form": "dbx_main.ca_bronze.self_declarations"
    },

    # Databricks CLI auth config (used when USE_DATABRICKS_CLI=True)
    # These can be set via environment variables: DATABRICKS_HOST, DATABRICKS_HTTP_PATH
    "databricks_host": os.environ.get("DATABRICKS_HOST", ""),
    "http_path": os.environ.get("DATABRICKS_HTTP_PATH", ""),
}

INTERNAL_WATCHLIST_TABLE = "dbx_main.ca_bronze.cf_watchlist"
INTERNAL_WATCHLIST_NAME_COL = "Name"

# ============================================================
# SAFE JSON PARSER
# ============================================================


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

# ============================================================
# DOCUMENT INTELLIGENCE
# ============================================================


@st.cache_data(ttl=3600)
def extract_text_from_pdf(pdf_bytes):
    client = DocumentIntelligenceClient(
        CONFIG["endpoint"],
        AzureKeyCredential(CONFIG["api_key"])
    )
    poller = client.begin_analyze_document("prebuilt-layout", body=pdf_bytes)
    return poller.result().content or ""

# ============================================================
# LLM CALL
# ============================================================


def call_model(prompt):
    r = requests.post(
        f"{CONFIG['workspace_url']}/serving-endpoints/{CONFIG['model_endpoint']}/invocations",
        headers={
            "Authorization": f"Bearer {CONFIG['pat_token']}",
            "Content-Type": "application/json"
        },
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

# ============================================================
# DOCUMENT TYPE DETECTION
# ============================================================


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

# ============================================================
# FIELD EXTRACTION
# ============================================================


def extract_fields(text, doc_type):
    schemas = {
        "Bank Customer Form": '{"applicant_name":"","full_address":"","phone_number":"","email":"","amount":"","beneficiary_names":[]}',
        "Bank Statement": '{"account_holder_name":"","account_number":"","bank_name":"","statement_period":"","total_deposits":"","total_withdrawals":""}',
        "Self Declaration Form": '{"declarant_name":"","purpose_of_declaration":"","address":"","date":"","signature_present":""}'
    }
    raw = call_model(f"Extract JSON:\n{schemas[doc_type]}\nTEXT:\n{text}")
    return safe_json_from_llm(raw)

# ============================================================
# SAVE TO DELTA
# ============================================================


def save_to_delta(fields, doc_type, case_id):
    table = CONFIG["delta_tables"][doc_type]
    record = fields.copy()
    record["case_id"] = case_id
    record["ingestion_ts"] = datetime.now(timezone.utc).isoformat()

    def esc(v): return str(v).replace("'", "''")
    cols = ", ".join(record.keys())
    vals = ", ".join([f"'{esc(v)}'" for v in record.values()])

    requests.post(
        f"{CONFIG['workspace_url']}/api/2.0/sql/statements",
        headers={
            "Authorization": f"Bearer {CONFIG['pat_token']}",
            "Content-Type": "application/json"
        },
        json={
            "warehouse_id": CONFIG["sql_warehouse_id"],
            "statement": f"INSERT INTO {table} ({cols}) VALUES ({vals})",
            "wait_timeout": "30s"
        }
    ).raise_for_status()


def save_aml_report(case_id, customer_name, document_type, aml_report):
    aml_json = json.dumps(aml_report).replace("'", "''")

    statement = f"""
        INSERT INTO clientadvisor.ca_data.aml_reports
        (case_id, customer_name, document_type, risk_score, aml_json, created_at)
        VALUES (
            '{case_id}',
            '{customer_name.replace("'", "''")}',
            '{document_type}',
            '{aml_report.get("risk_score", "REVIEW")}',
            '{aml_json}',
            current_timestamp()
        )
    """

    r = requests.post(
        f"{CONFIG['workspace_url']}/api/2.0/sql/statements",
        headers={
            "Authorization": f"Bearer {CONFIG['pat_token']}",
            "Content-Type": "application/json"
        },
        json={
            "warehouse_id": CONFIG["sql_warehouse_id"],
            "statement": statement,
            "wait_timeout": "30s"
        }
    )

    r.raise_for_status()
    result = r.json()

    # 🚨 CRITICAL CHECK
    if result.get("status", {}).get("state") != "SUCCEEDED":
        raise Exception(f"AML INSERT FAILED: {result}")


# ============================================================
# FETCH NAME
# ============================================================
def fetch_name(doc_type, case_id):
    table = CONFIG["delta_tables"][doc_type]

    # Explicit name column per document type
    name_column_map = {
        "Bank Customer Form": "applicant_name",
        "Bank Statement": "account_holder_name",
        "Self Declaration Form": "declarant_name"
    }

    name_col = name_column_map[doc_type]

    r = requests.post(
        f"{CONFIG['workspace_url']}/api/2.0/sql/statements",
        headers={
            "Authorization": f"Bearer {CONFIG['pat_token']}",
            "Content-Type": "application/json"
        },
        json={
            "warehouse_id": CONFIG["sql_warehouse_id"],
            "statement": f"""
                SELECT {name_col}
                FROM {table}
                WHERE case_id = '{case_id}'
                LIMIT 1
            """,
            "wait_timeout": "30s"
        }
    ).json()

    value = r["result"]["data_array"][0][0]

    if not value or not str(value).strip():
        raise Exception(f"{name_col} is empty – cannot run AML")

    return value.strip()

# ============================================================
# AML TOOLS
# ============================================================


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

        r = requests.post(
            f"{CONFIG['workspace_url']}/api/2.0/sql/statements",
            headers={
                "Authorization": f"Bearer {CONFIG['pat_token']}",
                "Content-Type": "application/json"
            },
            json={
                "warehouse_id": CONFIG["sql_warehouse_id"],
                "statement": statement,
                "wait_timeout": "30s"
            }
        ).json()

        rows = r.get("result", {}).get("data_array", [])

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

# ============================================================
# AML REPORT
# ============================================================


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


###################################################################
# ECDD Module - Enhanced Version (Ported from ECDD Agent)
###################################################################

# ECDD Delta Tables Configuration
ECDD_CONFIG = {
    "aml_source_table": "dbx_main.ca_bronze.preliminarysearch_result",
    "ecdd_reports_table": "dbx_main.ca_bronze.ecdd_reports",
    "volumes_path": "/Volumes/dbx_main/ca_bronze/ecdd_pdfs",
    "local_fallback_path": "./exports"
}

# ============================================================
# ECDD DATA CLASSES (Enhanced - matches schemas.py)
# ============================================================


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


# ============================================================
# READ AML TABLE
# ============================================================

# Toggle: Set to True to use Databricks CLI auth, False for REST API
USE_DATABRICKS_CLI = True

# CLI-based connection (uses `databricks auth login`)
_db_connection = None


def get_databricks_connection():
    """
    Get or create a Databricks SQL connection using CLI authentication.
    Requires: databricks-sql-connector, databricks auth login
    """
    global _db_connection
    if _db_connection is None:
        try:
            from databricks import sql

            # Uses CLI token automatically via DefaultCredentialProvider
            _db_connection = sql.connect(
                server_hostname=CONFIG.get(
                    "databricks_host", os.environ.get("DATABRICKS_HOST", "")),
                http_path=CONFIG.get("http_path", os.environ.get(
                    "DATABRICKS_HTTP_PATH", "")),
            )
        except ImportError:
            raise ImportError(
                "databricks-sql-connector not installed. Run: pip install databricks-sql-connector")
        except Exception as e:
            raise Exception(f"Failed to connect via CLI auth: {e}")
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


def execute_sql_rest(statement: str) -> dict:
    """Execute SQL statement against Databricks using REST API (PAT token)."""
    r = requests.post(
        f"{CONFIG['workspace_url']}/api/2.0/sql/statements",
        headers={
            "Authorization": f"Bearer {CONFIG['pat_token']}",
            "Content-Type": "application/json"
        },
        json={
            "warehouse_id": CONFIG["sql_warehouse_id"],
            "statement": statement,
            "wait_timeout": "30s"
        }
    )
    r.raise_for_status()
    return r.json()


def execute_sql(statement: str) -> dict:
    """
    Execute SQL statement against Databricks.
    Uses CLI auth if USE_DATABRICKS_CLI=True, otherwise REST API.
    """
    if USE_DATABRICKS_CLI:
        # CLI mode returns list of dicts - wrap in result format for compatibility
        rows = execute_sql_cli(statement)
        if not rows:
            return {"result": {"data_array": [], "schema": {"columns": []}}}
        columns = list(rows[0].keys()) if rows else []
        data_array = [[row.get(c) for c in columns] for row in rows]
        return {
            "result": {
                "data_array": data_array,
                "schema": {"columns": [{"name": c} for c in columns]}
            }
        }
    else:
        return execute_sql_rest(statement)


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

    def esc(val: str) -> str:
        return (str(val) if val else "").replace("'", "''")

    # Build structured JSON columns - ecdd_level is stored INSIDE ecdd_assessment_json
    compliance_flags_json = json.dumps(
        assessment.compliance_flags.to_dict()).replace("'", "''")

    ecdd_assessment_json = json.dumps({
        "client_type": assessment.client_type,
        "client_category": assessment.client_category,
        "overall_ecdd_level": assessment.overall_ecdd_level,
        "source_of_wealth": assessment.source_of_wealth,
        "source_of_funds": assessment.source_of_funds,
        "recommendations": assessment.recommendations,
        "required_actions": assessment.required_actions,
        "report_text": assessment.report_text,
        "rm_guidance": assessment.rm_guidance
    }).replace("'", "''")

    document_checklist_json = json.dumps({
        "identity_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.identity_documents],
        "source_of_wealth_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.source_of_wealth_documents],
        "source_of_funds_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.source_of_funds_documents],
        "compliance_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.compliance_documents],
        "additional_documents": [{"name": d.document_name, "priority": d.priority, "instructions": d.special_instructions} for d in checklist.additional_documents]
    }).replace("'", "''")

    questionnaire_responses_json = json.dumps(responses).replace("'", "''")

    # Note: No standalone ecdd_level column - it's inside ecdd_assessment_json
    statement = f"""
        INSERT INTO {ECDD_CONFIG['ecdd_reports_table']}
        (session_id, case_id, customer_name, compliance_flags_json, 
         ecdd_assessment_json, document_checklist_json, questionnaire_responses_json, status, created_at, updated_at)
        VALUES (
            '{session_id}',
            '{esc(case_id)}',
            '{esc(customer_name)}',
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


# ============================================================
# PDF EXPORT (Enhanced - matches exporter.py)
# ============================================================

# Bank-style color palette
BANK_COLORS = {
    "primary": "#4aa3ff",
    "secondary": "#69b1ff",
    "header_bg": "#829fe2",
    "table_alt": "#8eb0e6",
    "table_header": "#7e9acf",
    "text": "#0b0c0c",
    "text_light": "#121314",
    "border": "#2b3547",
    "success": "#28a745",
    "warning": "#f39c12",
    "danger": "#dc3545",
}


def generate_ecdd_pdf(
    customer_name: str,
    case_id: str,
    assessment: ECDDAssessment,
    checklist: DocumentChecklist,
    responses: Dict[str, str],
    questionnaire: DynamicQuestionnaire = None
) -> bytes:
    """
    Generate professional PDF report for ECDD assessment.
    Matches the exporter.py bank-style format.
    """
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer, pagesize=A4,
            topMargin=65, bottomMargin=60, leftMargin=30, rightMargin=30
        )
        styles = getSampleStyleSheet()

        # Custom bank-style styles
        styles.add(ParagraphStyle(
            name='BankTitle', parent=styles['Heading1'], fontSize=22,
            textColor=colors.HexColor(BANK_COLORS["text"]), alignment=TA_CENTER,
            spaceAfter=4, fontName='Helvetica-Bold', leading=26
        ))
        styles.add(ParagraphStyle(
            name='BankSubtitle', parent=styles['Heading2'], fontSize=11,
            textColor=colors.HexColor(BANK_COLORS["text_light"]), alignment=TA_CENTER,
            spaceAfter=16, fontName='Helvetica'
        ))
        styles.add(ParagraphStyle(
            name='SectionHeader', parent=styles['Heading2'], fontSize=14,
            textColor=colors.HexColor(BANK_COLORS["text"]), spaceBefore=18, spaceAfter=8,
            fontName='Helvetica-Bold'
        ))
        styles.add(ParagraphStyle(
            name='BankBody', parent=styles['Normal'], fontSize=10,
            textColor=colors.HexColor(BANK_COLORS["text"]), alignment=TA_JUSTIFY,
            spaceAfter=6, fontName='Helvetica', leading=14
        ))
        styles.add(ParagraphStyle(
            name='BankBullet', parent=styles['Normal'], fontSize=10,
            textColor=colors.HexColor(BANK_COLORS["text"]), leftIndent=20, spaceAfter=4,
            fontName='Helvetica', bulletIndent=10
        ))
        styles.add(ParagraphStyle(
            name='BankFooter', parent=styles['Normal'], fontSize=8,
            textColor=colors.HexColor(BANK_COLORS["text_light"]), alignment=TA_CENTER
        ))

        story = []

        # Title section
        story.append(Spacer(1, 20))
        story.append(
            Paragraph("ENHANCED CLIENT DUE DILIGENCE REPORT", styles['BankTitle']))
        story.append(
            Paragraph("Private Banking & Wealth Management", styles['BankSubtitle']))
        story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor(
            BANK_COLORS["primary"]), spaceAfter=12))

        # Client Information
        story.append(Paragraph("CLIENT INFORMATION", styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
            BANK_COLORS["primary"]), spaceAfter=8))

        client_data = [
            ["Customer Name:", str(customer_name)],
            ["Case ID:", str(case_id)[:12] + "..."],
            ["Client Type:", str(assessment.client_type)],
            ["Category:", str(assessment.client_category)],
            ["Date:", datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M UTC")],
        ]
        client_table = Table(client_data, colWidths=[120, 350])
        client_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1),
             colors.HexColor(BANK_COLORS["header_bg"])),
            ('BOX', (0, 0), (-1, -1), 1,
             colors.HexColor(BANK_COLORS["border"])),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 10),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ]))
        story.append(client_table)
        story.append(Spacer(1, 12))

        # ECDD Level
        story.append(Paragraph("OVERALL ECDD LEVEL", styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
            BANK_COLORS["primary"]), spaceAfter=8))

        level_colors_map = {
            "LOW": BANK_COLORS["success"], "MEDIUM": BANK_COLORS["warning"], "HIGH": BANK_COLORS["danger"]}
        level_color = level_colors_map.get(
            assessment.overall_ecdd_level.upper(), BANK_COLORS["text"])
        story.append(Paragraph(
            f"<b>ECDD Level:</b> <font color='{level_color}'>{assessment.overall_ecdd_level.upper()}</font>",
            styles['BankBody']
        ))
        story.append(Spacer(1, 8))

        # Compliance Flags
        story.append(Paragraph("COMPLIANCE FLAGS", styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
            BANK_COLORS["primary"]), spaceAfter=8))

        flags = assessment.compliance_flags
        flag_items = [
            ("PEP", flags.pep), ("Sanctions", flags.sanctions),
            ("Adverse Media", flags.adverse_media), ("High-Risk Jurisdiction",
                                                     flags.high_risk_jurisdiction),
            ("Watchlist Hit", flags.watchlist_hit), ("SOW Concerns",
                                                     flags.source_of_wealth_concerns),
            ("SOF Concerns", flags.source_of_funds_concerns), ("Complex Ownership",
                                                               flags.complex_ownership),
        ]
        flag_data = []
        for i in range(0, len(flag_items), 2):
            row = []
            for j in range(2):
                if i + j < len(flag_items):
                    name, status = flag_items[i + j]
                    indicator = "⚠ YES" if status else "✓ No"
                    row.extend([name + ":", indicator])
                else:
                    row.extend(["", ""])
            flag_data.append(row)

        flag_table = Table(flag_data, colWidths=[130, 70, 130, 70])
        flag_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1),
             colors.HexColor(BANK_COLORS["header_bg"])),
            ('BOX', (0, 0), (-1, -1), 0.5,
             colors.HexColor(BANK_COLORS["border"])),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ]))
        story.append(flag_table)
        story.append(Spacer(1, 12))

        # Source of Wealth
        story.append(Paragraph("SOURCE OF WEALTH", styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
            BANK_COLORS["secondary"]), spaceAfter=8))
        sow_text = assessment.source_of_wealth.get("summary", "") if isinstance(
            assessment.source_of_wealth, dict) else str(assessment.source_of_wealth)
        story.append(
            Paragraph(sow_text or "No information provided.", styles['BankBody']))

        # Source of Funds
        story.append(Paragraph("SOURCE OF FUNDS", styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
            BANK_COLORS["secondary"]), spaceAfter=8))
        sof_text = assessment.source_of_funds.get("summary", "") if isinstance(
            assessment.source_of_funds, dict) else str(assessment.source_of_funds)
        story.append(
            Paragraph(sof_text or "No information provided.", styles['BankBody']))

        # Recommendations
        if assessment.recommendations:
            story.append(Paragraph("RECOMMENDATIONS", styles['SectionHeader']))
            story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
                BANK_COLORS["secondary"]), spaceAfter=8))
            for i, rec in enumerate(assessment.recommendations, 1):
                story.append(Paragraph(f"{i}. {rec}", styles['BankBullet']))

        # Required Actions
        if assessment.required_actions:
            story.append(Paragraph("REQUIRED ACTIONS",
                         styles['SectionHeader']))
            story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
                BANK_COLORS["secondary"]), spaceAfter=8))
            for i, action in enumerate(assessment.required_actions, 1):
                story.append(Paragraph(f"{i}. {action}", styles['BankBullet']))

        # Document Checklist
        story.append(Paragraph("DOCUMENT CHECKLIST", styles['SectionHeader']))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
            BANK_COLORS["secondary"]), spaceAfter=8))

        doc_categories = [
            ("Identity Documents", checklist.identity_documents),
            ("Source of Wealth Documents", checklist.source_of_wealth_documents),
            ("Source of Funds Documents", checklist.source_of_funds_documents),
            ("Compliance Documents", checklist.compliance_documents),
            ("Additional Documents", checklist.additional_documents),
        ]
        for cat_name, docs in doc_categories:
            if docs:
                story.append(
                    Paragraph(f"<b>{cat_name}</b>", styles['BankBody']))
                for doc_item in docs:
                    priority_badge = {"required": "🔴", "recommended": "🟡", "optional": "🟢"}.get(
                        doc_item.priority.lower(), "⚪")
                    story.append(Paragraph(
                        f"  ☐ {doc_item.document_name} {priority_badge}", styles['BankBullet']))
                    if doc_item.special_instructions:
                        story.append(
                            Paragraph(f"    <i>{doc_item.special_instructions}</i>", styles['BankBullet']))

        # Questionnaire Responses
        if responses:
            story.append(Paragraph("QUESTIONNAIRE RESPONSES",
                         styles['SectionHeader']))
            story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
                BANK_COLORS["secondary"]), spaceAfter=8))
            for field_id, answer in responses.items():
                if answer and str(answer).strip():
                    # Try to get question text from questionnaire
                    q_text = field_id.replace("_", " ").title()
                    if questionnaire:
                        for q in questionnaire.all_questions():
                            if q.field_id == field_id:
                                q_text = q.question_text
                                break
                    story.append(
                        Paragraph(f"<b>Q:</b> {q_text}", styles['BankBody']))
                    story.append(
                        Paragraph(f"<b>A:</b> {answer}", styles['BankBody']))
                    story.append(Spacer(1, 4))

        # Full Report Text (narrative)
        if assessment.report_text:
            story.append(Paragraph("DETAILED ASSESSMENT",
                         styles['SectionHeader']))
            story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(
                BANK_COLORS["secondary"]), spaceAfter=8))
            # Parse markdown-like content
            for para in assessment.report_text.split('\n\n'):
                if para.strip():
                    story.append(Paragraph(para.strip(), styles['BankBody']))

        # Footer
        story.append(Spacer(1, 30))
        story.append(HRFlowable(width="100%", thickness=0.5,
                     color=colors.HexColor(BANK_COLORS["border"]), spaceAfter=8))
        story.append(Paragraph(
            f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}", styles['BankFooter']))
        story.append(Paragraph(
            "CONFIDENTIAL – For Internal Compliance Use Only", styles['BankFooter']))

        doc.build(story)
        return buffer.getvalue()

    except ImportError:
        # Fallback to simple text if reportlab not available
        sow_text = assessment.source_of_wealth.get("summary", "") if isinstance(
            assessment.source_of_wealth, dict) else str(assessment.source_of_wealth)
        sof_text = assessment.source_of_funds.get("summary", "") if isinstance(
            assessment.source_of_funds, dict) else str(assessment.source_of_funds)
        text = f"""
ENHANCED CLIENT DUE DILIGENCE REPORT
=====================================
Customer: {customer_name}
Case ID: {case_id}
ECDD Level: {assessment.overall_ecdd_level}
Client Type: {assessment.client_type}

SOURCE OF WEALTH
{sow_text}

SOURCE OF FUNDS
{sof_text}

RECOMMENDATIONS
{chr(10).join(f'- {r}' for r in assessment.recommendations)}

REQUIRED ACTIONS
{chr(10).join(f'- {a}' for a in assessment.required_actions)}

Generated: {datetime.now(timezone.utc).isoformat()}
CONFIDENTIAL – For Internal Compliance Use Only
"""
        return text.encode('utf-8')


def save_pdf_to_volumes(session_id: str, pdf_bytes: bytes, filename: str) -> str:
    """
    Save PDF to Databricks Volumes using SDK (CLI auth) or REST API.
    Matches databricks.py write_pdf_to_volume.
    """
    full_path = f"{ECDD_CONFIG['volumes_path']}/{session_id}/{filename}"

    if USE_DATABRICKS_CLI:
        # Use Databricks SDK with CLI auth
        try:
            from databricks.sdk import WorkspaceClient

            host = CONFIG.get("databricks_host",
                              os.environ.get("DATABRICKS_HOST", ""))
            w = WorkspaceClient(host=host) if host else WorkspaceClient()

            # Upload file - SDK expects file-like object
            w.files.upload(full_path, io.BytesIO(pdf_bytes), overwrite=True)
            return full_path

        except ImportError:
            pass  # Fall through to REST API
        except Exception as e:
            print(f"SDK upload failed: {e}, trying REST API...")

    # REST API fallback
    try:
        r = requests.put(
            f"{CONFIG['workspace_url']}/api/2.0/fs/files{full_path}",
            headers={
                "Authorization": f"Bearer {CONFIG['pat_token']}",
                "Content-Type": "application/octet-stream"
            },
            data=pdf_bytes
        )
        if r.status_code in [200, 201, 204]:
            return full_path
        else:
            raise Exception(
                f"Volume upload failed: {r.status_code} - {r.text}")
    except Exception as e:
        # Local fallback
        local_dir = os.path.join(ECDD_CONFIG.get(
            "local_fallback_path", "./exports"), session_id)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, filename)
        with open(local_path, 'wb') as f:
            f.write(pdf_bytes)
        return local_path


# ============================================================
# UI NAVIGATION
# ============================================================
st.sidebar.title("🗂️ Navigation")

page = st.sidebar.radio(
    "Go to:",
    [
        "1️⃣ Preliminary AML Screening",
        "2️⃣ ECDD Screening (Source of Wealth)"
    ]
)

st.title("🛡️ Onboarding AI")

# ============================================================
# PAGE 1 – AML SCREENING
# ============================================================
if page == "1️⃣ Preliminary AML Screening":

    st.header("🔍 Preliminary AML Screening")

    uploaded = st.file_uploader(
        "📄 Upload onboarding document (PDF)", type="pdf")

    if uploaded and st.button("🚀 Run AML Screening"):
        case_id = str(uuid.uuid4())

        with st.spinner("Processing document…"):
            text = extract_text_from_pdf(uploaded.read())
            doc_type = detect_document_type(text)
            fields = extract_fields(text, doc_type)
            save_to_delta(fields, doc_type, case_id)
            name = fetch_name(doc_type, case_id)

        with st.spinner("Running AML checks…"):
            aml_report = agentic_aml_screen(name)

            save_aml_report(
                case_id=case_id,
                customer_name=name,
                document_type=doc_type,
                aml_report=aml_report
            )

        st.success("AML Screening Completed")

        st.json(aml_report)

# -----------------------------------
# PAGE 2 — ECDD SCREENING (Enhanced)
# -----------------------------------
if page == "2️⃣ ECDD Screening (Source of Wealth)":

    st.header("🧾 ECDD – Enhanced Customer Due Diligence")

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

            with st.spinner("🤖 Auto-generating dynamic ECDD questionnaire..."):
                st.session_state.ecdd_questionnaire = generate_ecdd_questionnaire(
                    aml_json, customer_name, case)
            st.success("Dynamic questionnaire generated!")
            st.rerun()

        # TABS for ECDD workflow
        tab1, tab2, tab3, tab4 = st.tabs(
            ["📝 Questionnaire", "📊 Assessment Report", "📂 Document Checklist", "📤 PDF Export"])

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
                    with st.spinner("Generating comprehensive ECDD assessment..."):
                        assessment, checklist = generate_ecdd_assessment(
                            customer_name, aml_json, responses, case
                        )
                        st.session_state.ecdd_assessment = assessment
                        st.session_state.ecdd_checklist = checklist

                        # Save to Delta
                        session_id = save_ecdd_to_delta(
                            case, customer_name, assessment, checklist, responses
                        )
                        if session_id:
                            st.session_state.ecdd_session_id = session_id
                            st.success(
                                f"✅ Assessment saved to Delta! Session: {session_id[:8]}...")
                        else:
                            st.warning(
                                "⚠️ Assessment generated but Delta save failed. Check table schema and connection.")

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

        # ==========================================
        # TAB 4: PDF EXPORT
        # ==========================================
        with tab4:
            if st.session_state.ecdd_assessment and st.session_state.ecdd_checklist:
                assessment = st.session_state.ecdd_assessment
                checklist = st.session_state.ecdd_checklist
                questionnaire = st.session_state.ecdd_questionnaire
                responses = st.session_state.ecdd_responses
                session_id = st.session_state.ecdd_session_id or case

                st.subheader("📤 Export ECDD Assessment")

                col1, col2 = st.columns(2)

                with col1:
                    if st.button("📄 Generate PDF", use_container_width=True):
                        with st.spinner("Generating professional PDF report..."):
                            pdf_bytes = generate_ecdd_pdf(
                                customer_name, case, assessment, checklist, responses, questionnaire
                            )
                            st.session_state.ecdd_pdf_bytes = pdf_bytes
                        st.success("PDF generated!")

                with col2:
                    if st.session_state.ecdd_pdf_bytes:
                        st.download_button(
                            "⬇️ Download PDF",
                            data=st.session_state.ecdd_pdf_bytes,
                            file_name=f"ECDD_{customer_name.replace(' ', '_')}_{case[:8]}.pdf",
                            mime="application/pdf",
                            use_container_width=True
                        )

                if st.session_state.ecdd_pdf_bytes:
                    st.markdown("---")

                    if st.button("☁️ Save to Databricks Volumes", use_container_width=True):
                        with st.spinner("Uploading to Volumes..."):
                            filename = f"ECDD_{customer_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                            try:
                                path = save_pdf_to_volumes(
                                    session_id, st.session_state.ecdd_pdf_bytes, filename)
                                st.success(f"Saved to: {path}")
                            except Exception as e:
                                st.error(f"Upload failed: {e}")
            else:
                st.info("Generate an assessment report first to enable PDF export.")

    except Exception as e:
        st.error(f"Error loading AML cases: {e}")
        st.info("Make sure you have run AML screening first to create cases.")
