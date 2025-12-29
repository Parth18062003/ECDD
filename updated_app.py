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
    }
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
# ECDD Module - Enhanced Demo Version
###################################################################

# ECDD Delta Tables Configuration
ECDD_CONFIG = {
    "aml_source_table": "dbx_main.ca_bronze.preliminarysearch_result",
    "ecdd_reports_table": "dbx_main.ca_bronze.ecdd_reports",
    "ecdd_answers_table": "dbx_main.ca_bronze.ecdd_answers",
    "volumes_path": "/Volumes/dbx_main/ca_bronze/ecdd_pdfs"
}

# ============================================================
# ECDD DATA CLASSES (Simplified)
# ============================================================


class ECDDQuestionnaire:
    """Simple questionnaire container."""

    def __init__(self, data: dict):
        self.ice_breakers = data.get("ice_breakers", [])
        self.core_questions = data.get("core_questions", [])
        self.follow_up_questions = data.get("follow_up_questions", [])
        self.document_checklist = data.get("document_checklist", [])
        self.explanation = data.get("explanation_for_rm", "")
        self.client_type = data.get("client_type", "Individual")

    def all_questions(self) -> List[str]:
        return self.ice_breakers + self.core_questions + self.follow_up_questions


class ECDDReport:
    """Simple report container."""

    def __init__(self):
        self.client_type = ""
        self.sow_narrative = ""
        self.sof_summary = ""
        self.compliance_flags = {}
        self.recommendations = []
        self.ecdd_level = "MEDIUM"
        self.report_text = ""


# ============================================================
# READ AML TABLE
# ============================================================
def execute_sql(statement: str) -> dict:
    """Execute SQL statement against Databricks."""
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


def generate_ecdd_questions(aml_json, customer_name: str = ""):
    """Generate ECDD questionnaire using LLM based on AML findings."""
    prompt = f"""
You are an ECDD (Enhanced Customer Due Diligence) specialist.

Generate a structured Source of Wealth (SoW) questionnaire for customer: {customer_name}

INPUT (AML Result JSON):
{aml_json}

RULES:
1) Understand customer risk, sanctions, PEP, connected persons and risk score
2) Build conversation-driven questions — not interrogation style
3) Trace the journey from graduation → first income → how wealth accumulated
4) Questions must adapt to risk level
5) Infer client type from the profile (Employee, Business Owner, Retiree, etc.)

OUTPUT MUST BE CLEAN JSON ONLY WITH THIS STRUCTURE:

{{
  "client_type": "Inferred Type (e.g., Business Owner, Employee)",
  "ice_breakers": ["question1", "question2"],
  "core_questions": ["question1", "question2", "question3"],
  "follow_up_questions": ["question1", "question2"],
  "document_checklist": ["doc1", "doc2", "doc3"],
  "explanation_for_rm": "Guidance text for the RM"
}}

Return JSON only. No markdown. No commentary.
"""
    raw = call_model(prompt)
    data = safe_json_from_llm(raw)
    return ECDDQuestionnaire(data)


def save_ecdd_answers(case_id: str, qa_pairs: List[tuple]):
    """Save questionnaire answers to Delta table."""
    if not qa_pairs:
        return

    rows = []
    for q, a in qa_pairs:
        if a and a.strip():
            rows.append(
                f"('{case_id}', '{q.replace(chr(39), chr(39)+chr(39))}', '{a.replace(chr(39), chr(39)+chr(39))}', current_timestamp())")

    if not rows:
        return

    statement = f"""
        INSERT INTO {ECDD_CONFIG['ecdd_answers_table']} (case_id, question, answer, created_at)
        VALUES {", ".join(rows)}
    """
    execute_sql(statement)


def generate_ecdd_report(customer_name: str, aml_json: str, qa_pairs: List[tuple]) -> ECDDReport:
    """Generate full ECDD assessment report using LLM."""
    prompt = f"""
You are a compliance ECDD reviewer generating a formal assessment report.

INPUT:
Customer: {customer_name}

AML Findings:
{aml_json}

Questionnaire Responses:
{json.dumps([{"question": q, "answer": a} for q, a in qa_pairs if a], indent=2)}

Generate a comprehensive ECDD Assessment Report with:

1. CLIENT IDENTIFICATION SUMMARY
2. CLIENT TYPE CLASSIFICATION (Individual/Corporate/Trust/HNW)
3. SOURCE OF WEALTH NARRATIVE - explain HOW wealth was accumulated
4. SOURCE OF FUNDS SUMMARY - where current transaction funds come from
5. COMPLIANCE FLAGS (PEP status, sanctions, adverse media findings)
6. ECDD LEVEL (LOW/MEDIUM/HIGH based on overall profile)
7. RECOMMENDATIONS for the compliance team

OUTPUT AS JSON:
{{
    "client_type": "High Net Worth Individual",
    "client_category": "Individual",
    "sow_narrative": "Full source of wealth narrative...",
    "sof_summary": "Source of funds summary...",
    "compliance_flags": {{
        "pep": false,
        "sanctions": false,
        "adverse_media": false,
        "high_risk_jurisdiction": false
    }},
    "ecdd_level": "MEDIUM",
    "recommendations": ["recommendation1", "recommendation2"],
    "report_text": "Full formal narrative report text suitable for PDF..."
}}

Return JSON only. No markdown.
"""
    raw = call_model(prompt)
    data = safe_json_from_llm(raw)

    report = ECDDReport()
    report.client_type = data.get("client_type", "Unknown")
    report.sow_narrative = data.get("sow_narrative", "")
    report.sof_summary = data.get("sof_summary", "")
    report.compliance_flags = data.get("compliance_flags", {})
    report.recommendations = data.get("recommendations", [])
    report.ecdd_level = data.get("ecdd_level", "MEDIUM")
    report.report_text = data.get("report_text", "")
    return report


def save_ecdd_report_to_delta(case_id: str, customer_name: str, report: ECDDReport, questionnaire: ECDDQuestionnaire):
    """Save structured ECDD output to Delta table."""
    session_id = str(uuid.uuid4())

    assessment_json = json.dumps({
        "client_type": report.client_type,
        "sow_narrative": report.sow_narrative,
        "sof_summary": report.sof_summary,
        "ecdd_level": report.ecdd_level,
        "recommendations": report.recommendations,
        "report_text": report.report_text
    }).replace("'", "''")

    flags_json = json.dumps(report.compliance_flags).replace("'", "''")

    checklist_json = json.dumps({
        "documents": questionnaire.document_checklist
    }).replace("'", "''")

    statement = f"""
        INSERT INTO {ECDD_CONFIG['ecdd_reports_table']}
        (session_id, case_id, customer_name, ecdd_level, compliance_flags_json, 
         ecdd_assessment_json, document_checklist_json, status, created_at)
        VALUES (
            '{session_id}',
            '{case_id}',
            '{customer_name.replace("'", "''")}',
            '{report.ecdd_level}',
            '{flags_json}',
            '{assessment_json}',
            '{checklist_json}',
            'completed',
            current_timestamp()
        )
    """
    execute_sql(statement)
    return session_id


def generate_ecdd_pdf(customer_name: str, case_id: str, report: ECDDReport,
                      questionnaire: ECDDQuestionnaire, qa_pairs: List[tuple]) -> bytes:
    """Generate PDF report for ECDD assessment."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.lib.enums import TA_CENTER, TA_LEFT
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4,
                                topMargin=50, bottomMargin=50)
        styles = getSampleStyleSheet()

        # Custom styles
        title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18,
                                     alignment=TA_CENTER, spaceAfter=20)
        section_style = ParagraphStyle('Section', parent=styles['Heading2'], fontSize=14,
                                       spaceBefore=15, spaceAfter=10, textColor=colors.HexColor("#1b3a5f"))
        body_style = ParagraphStyle('Body', parent=styles['Normal'], fontSize=10,
                                    spaceAfter=8, leading=14)

        story = []

        # Title
        story.append(Paragraph("ECDD Assessment Report", title_style))
        story.append(Paragraph(f"Customer: {customer_name} | Case ID: {case_id[:8]}...",
                               ParagraphStyle('Subtitle', alignment=TA_CENTER, fontSize=11)))
        story.append(Spacer(1, 20))

        # ECDD Level Badge
        level_colors = {"LOW": "#28a745",
                        "MEDIUM": "#f39c12", "HIGH": "#dc3545"}
        level_color = level_colors.get(report.ecdd_level.upper(), "#666")
        story.append(Paragraph(
            f"<b>ECDD Level:</b> <font color='{level_color}'>{report.ecdd_level.upper()}</font>", body_style))
        story.append(
            Paragraph(f"<b>Client Type:</b> {report.client_type}", body_style))
        story.append(Spacer(1, 15))

        # Source of Wealth
        story.append(Paragraph("SOURCE OF WEALTH", section_style))
        story.append(
            Paragraph(report.sow_narrative or "No narrative provided.", body_style))

        # Source of Funds
        story.append(Paragraph("SOURCE OF FUNDS", section_style))
        story.append(
            Paragraph(report.sof_summary or "No summary provided.", body_style))

        # Compliance Flags
        story.append(Paragraph("COMPLIANCE FLAGS", section_style))
        flags = report.compliance_flags
        flag_text = []
        for flag, value in flags.items():
            status = "⚠️ YES" if value else "✓ No"
            flag_text.append(f"• {flag.replace('_', ' ').title()}: {status}")
        story.append(Paragraph("<br/>".join(flag_text)
                     if flag_text else "No flags identified.", body_style))

        # Recommendations
        story.append(Paragraph("RECOMMENDATIONS", section_style))
        for i, rec in enumerate(report.recommendations, 1):
            story.append(Paragraph(f"{i}. {rec}", body_style))

        # Document Checklist
        story.append(Paragraph("DOCUMENT CHECKLIST", section_style))
        for doc in questionnaire.document_checklist:
            story.append(Paragraph(f"☐ {doc}", body_style))

        # Questionnaire Responses
        story.append(Paragraph("QUESTIONNAIRE RESPONSES", section_style))
        for q, a in qa_pairs:
            if a and a.strip():
                story.append(Paragraph(f"<b>Q:</b> {q}", body_style))
                story.append(Paragraph(f"<b>A:</b> {a}", body_style))
                story.append(Spacer(1, 5))

        # Footer
        story.append(Spacer(1, 30))
        story.append(Paragraph(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
                               ParagraphStyle('Footer', fontSize=8, alignment=TA_CENTER)))
        story.append(Paragraph("CONFIDENTIAL – For Internal Compliance Use Only",
                               ParagraphStyle('Footer', fontSize=8, alignment=TA_CENTER)))

        doc.build(story)
        return buffer.getvalue()

    except ImportError:
        # Fallback to simple text if reportlab not available
        text = f"""
ECDD ASSESSMENT REPORT
======================
Customer: {customer_name}
Case ID: {case_id}
ECDD Level: {report.ecdd_level}
Client Type: {report.client_type}

SOURCE OF WEALTH
{report.sow_narrative}

SOURCE OF FUNDS
{report.sof_summary}

RECOMMENDATIONS
{chr(10).join(f'- {r}' for r in report.recommendations)}

Generated: {datetime.now(timezone.utc).isoformat()}
"""
        return text.encode('utf-8')


def save_pdf_to_volumes(case_id: str, pdf_bytes: bytes, filename: str) -> str:
    """Save PDF to Databricks Volumes."""
    import base64

    full_path = f"{ECDD_CONFIG['volumes_path']}/{case_id}/{filename}"

    try:
        # Use Files API to upload
        b64_content = base64.b64encode(pdf_bytes).decode('utf-8')

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
            # Fallback to local save
            raise Exception(f"Volume upload failed: {r.status_code}")

    except Exception as e:
        # Fallback: save locally
        local_dir = f"./exports/{case_id}"
        os.makedirs(local_dir, exist_ok=True)
        local_path = f"{local_dir}/{filename}"
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
    if "ecdd_report" not in st.session_state:
        st.session_state.ecdd_report = None
    if "ecdd_qa_pairs" not in st.session_state:
        st.session_state.ecdd_qa_pairs = []
    if "ecdd_selected_case" not in st.session_state:
        st.session_state.ecdd_selected_case = None
    if "ecdd_pdf_bytes" not in st.session_state:
        st.session_state.ecdd_pdf_bytes = None

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
            st.session_state.ecdd_report = None
            st.session_state.ecdd_qa_pairs = []
            st.session_state.ecdd_pdf_bytes = None

            with st.spinner("🤖 Auto-generating ECDD questionnaire..."):
                st.session_state.ecdd_questionnaire = generate_ecdd_questions(
                    aml_json, customer_name)
            st.success("Questionnaire generated!")
            st.rerun()

        # TABS for ECDD workflow
        tab1, tab2, tab3 = st.tabs(
            ["📝 Questionnaire", "📊 Assessment Report", "📤 PDF Export"])

        # ==========================================
        # TAB 1: QUESTIONNAIRE
        # ==========================================
        with tab1:
            if st.session_state.ecdd_questionnaire:
                q = st.session_state.ecdd_questionnaire

                st.markdown(f"**Inferred Client Type:** {q.client_type}")
                st.markdown("---")

                # Collect responses
                qa_inputs = []
                all_questions = q.all_questions()

                st.subheader("💬 Questions")
                for i, question in enumerate(all_questions):
                    answer = st.text_area(
                        f"{i+1}. {question}",
                        key=f"ecdd_q_{i}",
                        height=80
                    )
                    qa_inputs.append((question, answer))

                st.subheader("📂 Document Checklist")
                for doc in q.document_checklist:
                    st.checkbox(f"☐ {doc}", key=f"doc_{hash(doc)}")

                with st.expander("🧭 Guidance for RM"):
                    st.write(q.explanation)

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("💾 Save Answers", use_container_width=True):
                        st.session_state.ecdd_qa_pairs = qa_inputs
                        try:
                            save_ecdd_answers(case, qa_inputs)
                            st.success("Answers saved to Delta!")
                        except Exception as e:
                            st.warning(f"Could not save to Delta: {e}")

                with col2:
                    if st.button("📊 Generate Report", use_container_width=True, type="primary"):
                        st.session_state.ecdd_qa_pairs = qa_inputs
                        with st.spinner("Generating ECDD Assessment Report..."):
                            report = generate_ecdd_report(
                                customer_name, aml_json, qa_inputs)
                            st.session_state.ecdd_report = report

                            # Save to Delta
                            try:
                                session_id = save_ecdd_report_to_delta(
                                    case, customer_name, report, q
                                )
                                st.success(
                                    f"Report saved to Delta! Session: {session_id[:8]}...")
                            except Exception as e:
                                st.warning(f"Delta save failed: {e}")

                        st.rerun()
            else:
                if st.button("🔄 Generate Questionnaire"):
                    with st.spinner("Generating..."):
                        st.session_state.ecdd_questionnaire = generate_ecdd_questions(
                            aml_json, customer_name)
                    st.rerun()

        # ==========================================
        # TAB 2: ASSESSMENT REPORT
        # ==========================================
        with tab2:
            if st.session_state.ecdd_report:
                report = st.session_state.ecdd_report

                # ECDD Level badge
                level_colors = {"LOW": "#22c55e",
                                "MEDIUM": "#facc15", "HIGH": "#ef4444"}
                level_color = level_colors.get(
                    report.ecdd_level.upper(), "#666")

                st.markdown(f"""
                <div style="text-align:center;padding:20px">
                    <h3>ECDD Assessment Level</h3>
                    <div style="display:inline-block;background:{level_color};color:white;
                                font-size:24px;padding:12px 40px;border-radius:8px;font-weight:bold">
                        {report.ecdd_level.upper()}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                st.markdown(f"**Client Type:** {report.client_type}")
                st.markdown("---")

                # Source of Wealth
                st.subheader("💰 Source of Wealth")
                st.write(report.sow_narrative or "No narrative generated.")

                # Source of Funds
                st.subheader("💵 Source of Funds")
                st.write(report.sof_summary or "No summary generated.")

                # Compliance Flags
                st.subheader("🚩 Compliance Flags")
                cols = st.columns(4)
                flags = report.compliance_flags
                flag_names = ["pep", "sanctions",
                              "adverse_media", "high_risk_jurisdiction"]
                for i, flag in enumerate(flag_names):
                    with cols[i]:
                        value = flags.get(flag, False)
                        icon = "⚠️" if value else "✅"
                        color = "red" if value else "green"
                        st.markdown(f"<span style='color:{color}'>{icon} {flag.replace('_', ' ').title()}</span>",
                                    unsafe_allow_html=True)

                # Recommendations
                st.subheader("📋 Recommendations")
                for i, rec in enumerate(report.recommendations, 1):
                    st.markdown(f"{i}. {rec}")

                # Full report text
                with st.expander("📄 Full Report Text"):
                    st.write(
                        report.report_text or "No detailed report text available.")
            else:
                st.info(
                    "Complete the questionnaire and generate a report to view the assessment.")

        # ==========================================
        # TAB 3: PDF EXPORT
        # ==========================================
        with tab3:
            if st.session_state.ecdd_report and st.session_state.ecdd_questionnaire:
                report = st.session_state.ecdd_report
                q = st.session_state.ecdd_questionnaire
                qa_pairs = st.session_state.ecdd_qa_pairs

                st.subheader("📤 Export ECDD Assessment")

                col1, col2 = st.columns(2)

                with col1:
                    if st.button("📄 Generate PDF", use_container_width=True):
                        with st.spinner("Generating PDF..."):
                            pdf_bytes = generate_ecdd_pdf(
                                customer_name, case, report, q, qa_pairs
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
                                    case, st.session_state.ecdd_pdf_bytes, filename)
                                st.success(f"Saved to: {path}")
                            except Exception as e:
                                st.error(f"Upload failed: {e}")
            else:
                st.info("Generate an assessment report first to enable PDF export.")

    except Exception as e:
        st.error(f"Error loading AML cases: {e}")
        st.info("Make sure you have run AML screening first to create cases.")
