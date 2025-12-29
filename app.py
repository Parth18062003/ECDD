"""
ECDD Streamlit Application

Questionnaire-based Enhanced Client Due Diligence system.
Standalone application - no orchestrator dependencies.

Features:
- Client profile selection from Databricks
- Dynamic questionnaire generation
- Stakeholder review workflow
- PDF export to Volumes
"""

import streamlit as st
import asyncio
import json
import os
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional

# Import ECDD modules
from .schemas import (
    ClientProfile,
    QuestionnaireSession,
    DynamicQuestionnaire,
    ECDDAssessment,
    DocumentChecklist,
    SessionStatus,
    QuestionType,
)
from .agents import ECDDAgentCoordinator, get_coordinator
from .databricks import DatabricksConnector, get_databricks_connector
from .exporter import ECDDExporter
from .session import ECDDSessionManager, get_session_manager
from .utils import format_display_date, format_display_datetime, ensure_string

# Page configuration
st.set_page_config(
    page_title="ECDD Assessment System",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)


# =============================================================================
# PROFESSIONAL FINANCIAL STYLING
# =============================================================================

def load_custom_css():
    """Professional financial application styling - clean, compliance-focused."""
    st.markdown("""
    <style>
        /* Color palette - Professional financial */
        :root {
            --primary-navy: #0d1b2a;
            --secondary-navy: #1b3a5f;
            --accent-gold: #c9a227;
            --accent-gold-light: #e6c84a;
            --text-dark: #1a1a1a;
            --text-medium: #4a5568;
            --text-light: #718096;
            /* Avoid pure white backgrounds to remain readable in dark/high-contrast themes */
            --bg-white: var(--bg-off-white);
            --bg-off-white: #f8f9fa;
            --bg-light-gray: #e9ecef;
            --border-light: #dee2e6;
            --success: #28a745;
            --warning: #f39c12;
            --danger: #dc3545;
            --info: #17a2b8;
        }
        
        /* Main app background - clean white */
        .stApp {
            background: var(--bg-off-white);
        }
        
        /* Professional card styling */
        .info-card {
            background: var(--bg-off-white);
            border-radius: 8px;
            padding: 24px;
            margin: 12px 0;
            border: 1px solid var(--border-light);
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.04);
        }
        
        .info-card h3 {
            color: var(--primary-navy);
            margin-bottom: 16px;
            font-weight: 600;
            border-bottom: 2px solid var(--accent-gold);
            padding-bottom: 8px;
        }
        
        /* Status badges - muted professional */
        .status-badge {
            display: inline-block;
            padding: 6px 14px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .status-pending { background: #fff3cd; color: #856404; }
        .status-in-progress { background: #d1ecf1; color: #0c5460; }
        .status-completed { background: #d4edda; color: #155724; }
        .status-review { background: #e2e3e5; color: #383d41; }
        
        /* Section header - professional navy with gold accent */
        .section-header {
            background: var(--primary-navy);
            color: white;
            padding: 14px 24px;
            border-radius: 4px;
            margin: 24px 0 12px 0;
            border-left: 4px solid var(--accent-gold);
            font-weight: 600;
        }
        
        /* Question card styling */
        .question-card {
            background: var(--bg-off-white);
            border-left: 3px solid var(--secondary-navy);
            padding: 16px 20px;
            margin: 8px 0;
            border-radius: 0 4px 4px 0;
        }
        
        /* Risk rating badges - professional muted colors */
        .risk-low { 
            background: #d4edda; 
            color: #155724; 
            border: 1px solid #c3e6cb;
        }
        .risk-medium { 
            background: #fff3cd; 
            color: #856404; 
            border: 1px solid #ffeeba;
        }
        .risk-high { 
            background: #f8d7da; 
            color: #721c24; 
            border: 1px solid #f5c6cb;
        }
        .risk-critical { 
            background: #d32f2f; 
            color: white; 
            border: 1px solid #c62828;
        }
        
        /* Sidebar styling */
        section[data-testid="stSidebar"] {
            background: var(--primary-navy);
        }
        
        section[data-testid="stSidebar"] .stMarkdown {
            color: white;
        }
        
        /* Button styling */
        .stButton > button {
            background: var(--secondary-navy);
            color: white;
            border: none;
            font-weight: 500;
            transition: all 0.2s ease;
        }
        
        .stButton > button:hover {
            background: var(--primary-navy);
            border-color: var(--accent-gold);
        }
        
        /* Primary button with gold accent */
        .stButton > button[kind="primary"] {
            background: var(--accent-gold);
            color: var(--text-dark);
        }
        
        .stButton > button[kind="primary"]:hover {
            background: var(--accent-gold-light);
        }
        
        /* Tab styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            background: var(--bg-off-white);
            padding: 8px;
            border-radius: 4px;
        }
        
        .stTabs [data-baseweb="tab"] {
            background: var(--bg-light-gray);
            border-radius: 4px;
            padding: 8px 16px;
            font-weight: 500;
        }
        
        .stTabs [aria-selected="true"] {
            background: var(--primary-navy);
            color: white;
        }
        
        /* Expander styling */
        .streamlit-expanderHeader {
            background: var(--bg-off-white);
            border: 1px solid var(--border-light);
            border-radius: 4px;
        }
        
        /* Form inputs */
        .stTextInput > div > div > input,
        .stTextArea > div > div > textarea,
        .stSelectbox > div > div > select {
            border: 1px solid var(--border-light);
            border-radius: 4px;
        }
        
        /* Alert boxes */
        .stAlert {
            border-radius: 4px;
        }
        
        /* Progress bar */
        .stProgress > div > div {
            background: var(--accent-gold);
        }
        
        /* Header text */
        h1, h2, h3 {
            color: var(--primary-navy);
            font-weight: 600;
        }
        
        /* Clean table styling */
        .dataframe {
            border: 1px solid var(--border-light);
            border-radius: 4px;
        }
        
        .dataframe th {
            background: var(--primary-navy);
            color: white;
        }
    </style>
    """, unsafe_allow_html=True)


# =============================================================================
# INITIALIZATION
# =============================================================================

def init_session_state():
    """Initialize Streamlit session state."""
    defaults = {
        'page': 'home',
        'session_id': None,
        'client_profile': None,
        'questionnaire': None,
        'responses': {},
        'ecdd_assessment': None,
        'document_checklist': None,
        'coordinator': None,
        'databricks': None,
        'exporter': None,
        'session_manager': None,
    }

    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default

    # Initialize components
    if st.session_state.coordinator is None:
        st.session_state.coordinator = get_coordinator()
        try:
            asyncio.run(st.session_state.coordinator.initialize())
        except Exception as e:
            st.warning(f"Agent initialization pending: {e}")

    if st.session_state.databricks is None:
        st.session_state.databricks = get_databricks_connector()

    if st.session_state.exporter is None:
        st.session_state.exporter = ECDDExporter()
        st.session_state.exporter.set_databricks_connector(
            st.session_state.databricks)

    if st.session_state.session_manager is None:
        st.session_state.session_manager = get_session_manager()


# =============================================================================
# DEMO CLIENT PROFILE
# =============================================================================

def get_demo_client_profile() -> ClientProfile:
    """Get a demo client profile for testing."""
    from .schemas import IdentityProfile, PEPStatus, AdverseNewsItem

    return ClientProfile(
        customer_id="DEMO-001",
        customer_name="Alexandra Chen",
        identity=IdentityProfile(
            customer_id="DEMO-001",
            full_name="Alexandra Chen",
            nationality="Singapore",
            residence_country="Singapore",
            dob="1978-03-15",
            id_type="National ID",
            id_number="S7815234J",
            risk_segment="High Net Worth",
            is_etb=True
        ),
        pep_status=[
            PEPStatus(is_pep=True, pep_type="Family Member",
                      position_title="Spouse of Minister")
        ],
        adverse_news=[
            AdverseNewsItem(
                source_name="Financial Times",
                headline="Investment firm under regulatory review",
                category="regulatory"
            )
        ]
    )


# =============================================================================
# PAGE: HOME
# =============================================================================

def page_home():
    """Home page - start new questionnaire."""
    st.markdown('<div class="info-card">', unsafe_allow_html=True)
    st.markdown("## 📋 ECDD Questionnaire System")
    st.markdown("Enhanced Client Due Diligence questionnaire-based assessment.")
    st.markdown('</div>', unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown('<div class="info-card">', unsafe_allow_html=True)
        st.markdown("### Select Client Profile")

        # Profile source selection
        source = st.radio(
            "Profile Source",
            ["Demo Profile", "From Databricks", "Manual Entry"],
            horizontal=True
        )

        if source == "Demo Profile":
            st.info("Using demo client profile: Alexandra Chen (DEMO-001)")
            if st.button("▶️ Start Questionnaire with Demo Profile", use_container_width=True):
                profile = get_demo_client_profile()
                _start_session(profile)

        elif source == "From Databricks":
            search_term = st.text_input("🔍 Search by name or ID")

            if search_term:
                try:
                    profiles = st.session_state.databricks.search_profiles(
                        search_term)
                    if profiles:
                        selected = st.selectbox(
                            "Select profile",
                            profiles,
                            format_func=lambda p: f"{p.customer_name} ({p.customer_id})"
                        )
                        if st.button("▶️ Start Questionnaire", use_container_width=True):
                            _start_session(selected)
                    else:
                        st.warning("No profiles found")
                except Exception as e:
                    st.error(f"Databricks connection error: {e}")
                    st.info("Try using Demo Profile instead")

        else:  # Manual Entry
            with st.form("manual_profile"):
                customer_id = st.text_input("Customer ID*", "")
                customer_name = st.text_input("Customer Name*", "")
                nationality = st.text_input("Nationality", "")
                residence = st.text_input("Residence Country", "")

                if st.form_submit_button("▶️ Start Questionnaire"):
                    if customer_id and customer_name:
                        from .schemas import IdentityProfile
                        profile = ClientProfile(
                            customer_id=customer_id,
                            customer_name=customer_name,
                            identity=IdentityProfile(
                                customer_id=customer_id,
                                full_name=customer_name,
                                nationality=nationality,
                                residence_country=residence
                            )
                        )
                        _start_session(profile)
                    else:
                        st.error("Customer ID and Name are required")

        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="info-card">', unsafe_allow_html=True)
        st.markdown("### Recent Sessions")

        sessions = st.session_state.session_manager.list_sessions(limit=5)
        if sessions:
            for s in sessions:
                status_class = {
                    'pending': 'pending',
                    'questionnaire_generated': 'pending',
                    'in_progress': 'in-progress',
                    'reports_generated': 'completed',
                    'approved': 'completed',
                }.get(s['status'], 'pending')

                st.markdown(f"""
                <div style="padding: 8px; margin: 5px 0; background: var(--bg-off-white); border-radius: 5px;">
                    <strong>{s['customer_name']}</strong><br>
                    <small>{s['customer_id']} | 
                    <span class="status-badge status-{status_class}">{s['status']}</span></small>
                </div>
                """, unsafe_allow_html=True)

                if st.button(f"Resume", key=f"resume_{s['session_id'][:8]}"):
                    _resume_session(s['session_id'])
        else:
            st.info("No recent sessions")

        st.markdown('</div>', unsafe_allow_html=True)


def _start_session(profile: ClientProfile):
    """Start a new questionnaire session."""
    with st.spinner("Generating questionnaire..."):
        try:
            session = asyncio.run(
                st.session_state.coordinator.create_session(profile))
            st.session_state.session_id = session.session_id
            st.session_state.client_profile = profile
            st.session_state.questionnaire = session.questionnaire
            st.session_state.responses = {}
            st.session_state.page = 'questionnaire'

            # Save to session manager
            st.session_state.session_manager.create_session(
                profile,
                session.questionnaire,
                session_id=session.session_id
            )

            st.rerun()
        except Exception as e:
            st.error(f"Error starting session: {e}")
            traceback.print_exc()


def _resume_session(session_id: str):
    """Resume an existing session."""
    session = st.session_state.session_manager.get_session(session_id)
    if session:
        # Hydrate coordinator in-memory cache for this session
        try:
            st.session_state.coordinator.upsert_session(session)
        except Exception:
            pass

        st.session_state.session_id = session.session_id
        st.session_state.client_profile = ClientProfile(
            **session.client_profile) if session.client_profile else None
        st.session_state.questionnaire = session.questionnaire
        st.session_state.responses = session.responses or {}
        st.session_state.ecdd_assessment = session.ecdd_assessment
        st.session_state.document_checklist = session.document_checklist

        if session.status in [SessionStatus.REPORTS_GENERATED, SessionStatus.PENDING_REVIEW]:
            st.session_state.page = 'review'
        else:
            st.session_state.page = 'questionnaire'

        st.rerun()


# =============================================================================
# PAGE: QUESTIONNAIRE
# =============================================================================

def page_questionnaire():
    """Questionnaire page - fill out ECDD questions."""
    if not st.session_state.questionnaire:
        st.warning("No questionnaire loaded")
        if st.button("← Back to Home"):
            st.session_state.page = 'home'
            st.rerun()
        return

    questionnaire = st.session_state.questionnaire

    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"## 📋 ECDD Questionnaire")
        st.markdown(
            f"**Client:** {questionnaire.customer_name} ({questionnaire.customer_id})")
    with col2:
        if st.button("🏠 Home"):
            st.session_state.page = 'home'
            st.rerun()

    # Progress
    total_qs = questionnaire.get_total_questions()
    answered = len([r for r in st.session_state.responses.values() if r])
    st.progress(answered / total_qs if total_qs > 0 else 0,
                f"Progress: {answered}/{total_qs}")

    # Sections
    with st.form("questionnaire_form"):
        for section in questionnaire.sections:
            st.markdown(f"""
            <div class="section-header">
                {section.section_icon} {section.section_title}
            </div>
            """, unsafe_allow_html=True)

            if section.section_description:
                st.caption(section.section_description)

            for q in section.questions:
                _render_question(q)

        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            submitted = st.form_submit_button(
                "✅ Submit & Generate Assessment",
                use_container_width=True
            )

        if submitted:
            _submit_questionnaire()


def _render_question(q):
    """Render a single question field."""
    key = f"q_{q.field_id}"
    current = st.session_state.responses.get(q.field_id, "")

    label = q.question_text
    if q.required:
        label += " *"

    if q.question_type == QuestionType.TEXT:
        val = st.text_input(label, value=current, key=key, help=q.help_text)
    elif q.question_type == QuestionType.TEXTAREA:
        val = st.text_area(label, value=current, key=key, help=q.help_text)
    elif q.question_type == QuestionType.DROPDOWN:
        options = [""] + q.options
        idx = options.index(current) if current in options else 0
        val = st.selectbox(label, options, index=idx,
                           key=key, help=q.help_text)
    elif q.question_type == QuestionType.MULTIPLE_CHOICE:
        val = st.radio(label, q.options, key=key, help=q.help_text)
    elif q.question_type == QuestionType.CHECKBOX:
        current_list = current if isinstance(current, list) else []
        val = st.multiselect(
            label, q.options, default=current_list, key=key, help=q.help_text)
    elif q.question_type == QuestionType.YES_NO:
        options = ["", "Yes", "No"]
        idx = options.index(current) if current in options else 0
        val = st.selectbox(label, options, index=idx,
                           key=key, help=q.help_text)
    elif q.question_type == QuestionType.DATE:
        val = st.date_input(label, key=key, help=q.help_text)
        val = str(val) if val else ""
    elif q.question_type == QuestionType.NUMBER:
        val = st.number_input(label, value=float(
            current) if current else 0.0, key=key, help=q.help_text)
    elif q.question_type == QuestionType.CURRENCY:
        val = st.number_input(label, value=float(
            current) if current else 0.0, key=key, help=q.help_text)
    else:
        val = st.text_input(label, value=current, key=key, help=q.help_text)

    st.session_state.responses[q.field_id] = val


def _submit_questionnaire():
    """Submit questionnaire and generate assessment."""
    with st.spinner("Generating ECDD Assessment..."):
        try:
            session_id = st.session_state.session_id
            responses = st.session_state.responses

            # Generate assessment via coordinator
            assessment, checklist = asyncio.run(
                st.session_state.coordinator.submit_responses(
                    session_id, responses)
            )

            st.session_state.ecdd_assessment = assessment
            st.session_state.document_checklist = checklist

            # Update session manager
            st.session_state.session_manager.update_session(
                session_id,
                status=SessionStatus.REPORTS_GENERATED,
                responses=responses,
                ecdd_assessment=assessment,
                document_checklist=checklist,
                save_immediately=True
            )

            # Persist structured output (Delta if available, else local JSON fallback)
            try:
                output = asyncio.run(
                    st.session_state.coordinator.get_ecdd_output(session_id)
                )
                st.session_state.databricks.write_ecdd_output(output)
            except Exception as e:
                st.info(
                    f"Output persistence skipped (Databricks/local fallback unavailable): {e}")

            # Refresh client profile (may be enriched/merged by the coordinator)
            try:
                coord_session = st.session_state.coordinator.get_session(
                    session_id)
                if coord_session and coord_session.client_profile:
                    st.session_state.client_profile = ClientProfile(
                        **coord_session.client_profile)
            except Exception:
                pass

            st.session_state.page = 'review'
            st.success("Assessment generated successfully!")
            st.rerun()

        except Exception as e:
            st.error(f"Error generating assessment: {e}")
            traceback.print_exc()


# =============================================================================
# PAGE: STAKEHOLDER REVIEW
# =============================================================================

def page_review():
    """Stakeholder review page."""
    if not st.session_state.ecdd_assessment:
        st.warning("No assessment to review")
        if st.button("← Back to Home"):
            st.session_state.page = 'home'
            st.rerun()
        return

    assessment = st.session_state.ecdd_assessment
    checklist = st.session_state.document_checklist

    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("## 🔍 Stakeholder Review")
        st.markdown(
            f"**Client:** {st.session_state.questionnaire.customer_name}")
    with col2:
        if st.button("🏠 Home"):
            st.session_state.page = 'home'
            st.rerun()

    # Tabs - added History and Follow-up
    tabs = st.tabs(["📊 Summary", "👤 Client Profile", "📋 Assessment",
                   "📄 Documents", "🔄 Follow-up", "📜 History", "💬 Query", "📤 Export"])

    with tabs[0]:  # Summary
        _render_summary(assessment)

    with tabs[1]:  # Client Profile
        _render_client_profile_tab()

    with tabs[2]:  # Assessment
        _render_assessment(assessment)

    with tabs[3]:  # Documents
        _render_checklist(checklist)

    with tabs[4]:  # Follow-up
        _render_followup_tab()

    with tabs[5]:  # History
        _render_history_tab()

    with tabs[6]:  # Query
        _render_query_tab()

    with tabs[7]:  # Export
        _render_export_tab()

    # Review decision
    st.markdown("---")
    st.markdown("### Review Decision")

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("✅ Approve", use_container_width=True, type="primary"):
            _complete_review("approved")
    with col2:
        if st.button("⚠️ Escalate", use_container_width=True):
            _complete_review("escalated")
    with col3:
        if st.button("❌ Reject", use_container_width=True):
            _complete_review("rejected")


def _render_summary(assessment: ECDDAssessment):
    """Render assessment summary."""
    st.markdown('<div class="info-card">', unsafe_allow_html=True)

    # ECDD Level display
    level_colors = {
        "low": "#28a745",
        "medium": "#f39c12",
        "high": "#dc3545",
        "critical": "#9b2c2c"
    }
    level_value = assessment.overall_ecdd_rating.value
    level_color = level_colors.get(level_value, "#666")

    st.markdown(f"""
    <div style="text-align: center; padding: 20px;">
        <h2>ECDD Assessment Level</h2>
        <div style="display: inline-block; background: {level_color}; color: white; 
                    font-size: 20px; padding: 12px 40px; border-radius: 4px; font-weight: bold;">
            {level_value.upper()}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Client type
    if assessment.client_type:
        st.markdown(f"**Client Type:** {assessment.client_type}")

    # Compliance flags
    st.markdown("**Compliance Status:**")
    flags = assessment.compliance_flags
    flag_items = [
        ("PEP", flags.pep),
        ("Sanctions", flags.sanctions),
        ("Adverse Media", flags.adverse_media),
        ("High-Risk Jurisdiction", flags.high_risk_jurisdiction),
        ("Watchlist", flags.watchlist_hit),
    ]

    cols = st.columns(5)
    for i, (name, status) in enumerate(flag_items):
        with cols[i]:
            indicator = "⚠️" if status else "✓"
            color = "red" if status else "green"
            st.markdown(
                f"<span style='color:{color}'>{indicator} {name}</span>", unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


def _render_assessment(assessment: ECDDAssessment):
    """Render full assessment details."""
    st.markdown('<div class="info-card">', unsafe_allow_html=True)

    # Source of Wealth Summary
    if assessment.source_of_wealth:
        st.markdown("### Source of Wealth")
        sow = assessment.source_of_wealth
        if isinstance(sow, dict) and sow.get("summary"):
            st.markdown(sow["summary"])
        else:
            st.info("No source of wealth information provided yet.")

    # Source of Funds Summary
    if assessment.source_of_funds:
        st.markdown("### Source of Funds")
        sof = assessment.source_of_funds
        if isinstance(sof, dict) and sof.get("summary"):
            st.markdown(sof["summary"])
        else:
            st.info("No source of funds information provided yet.")

    # Detailed assessment narrative (cleaned up, no JSON)
    if assessment.report_text:
        st.markdown("### Assessment Summary")
        # Clean up report - remove JSON blocks and markdown code fences
        clean_text = assessment.report_text
        # Remove JSON code blocks
        import re
        clean_text = re.sub(r'```json[\s\S]*?```', '', clean_text)
        clean_text = re.sub(r'```documents[\s\S]*?```', '', clean_text)
        clean_text = re.sub(r'```[\s\S]*?```', '', clean_text)
        clean_text = clean_text.strip()

        if clean_text:
            st.markdown(clean_text)

    # Recommendations
    if assessment.recommendations:
        st.markdown("### Recommendations")
        for i, rec in enumerate(assessment.recommendations, 1):
            st.markdown(f"{i}. {rec}")

    # Required Actions
    if assessment.required_actions:
        st.markdown("### Required Actions")
        for action in assessment.required_actions:
            st.markdown(f"- {action}")

    st.markdown('</div>', unsafe_allow_html=True)


def _render_checklist(checklist: DocumentChecklist):
    """Render document checklist."""
    st.markdown('<div class="info-card">', unsafe_allow_html=True)
    st.markdown("### Document Checklist")

    categories = [
        ("🆔 Identity Documents", checklist.identity_documents),
        ("💰 Source of Wealth", checklist.source_of_wealth_documents),
        ("💵 Source of Funds", checklist.source_of_funds_documents),
        ("📋 Compliance", checklist.compliance_documents),
        ("📎 Additional", checklist.additional_documents),
    ]

    has_docs = any(docs for _, docs in categories)

    if has_docs:
        for title, docs in categories:
            if docs:
                st.markdown(f"**{title}:**")
                for doc in docs:
                    priority_emoji = {"required": "🔴", "recommended": "🟡", "optional": "🟢"}.get(
                        doc.priority.value, "⚪"
                    )
                    st.checkbox(
                        f"{priority_emoji} {doc.document_name}",
                        key=f"doc_{hash(doc.document_name)}"
                    )
                    if doc.special_instructions:
                        st.caption(f"   ↳ {doc.special_instructions}")
    else:
        # Provide standard documents if none generated
        st.info("📋 Standard documents required for ECDD:")
        standard_docs = [
            ("🆔 Identity Documents", [
                "Government-issued photo ID (passport/national ID)",
                "Proof of address (utility bill, bank statement - < 3 months old)"
            ]),
            ("💰 Source of Wealth", [
                "Employment contract or business ownership documents",
                "Tax returns or financial statements"
            ]),
            ("💵 Source of Funds", [
                "Bank statements (3-6 months)",
                "Investment portfolio statements"
            ]),
        ]

        for title, docs in standard_docs:
            st.markdown(f"**{title}:**")
            for doc in docs:
                st.checkbox(f"🔴 {doc}", key=f"std_{hash(doc)}")

    st.markdown('</div>', unsafe_allow_html=True)


def _render_client_profile_tab():
    """Render extracted/merged client profile used for reporting."""
    st.markdown('<div class="info-card">', unsafe_allow_html=True)
    st.markdown("### Client Profile (Extracted + Questionnaire-Enriched)")

    profile = st.session_state.client_profile
    if not profile:
        st.info("No client profile loaded")
        st.markdown('</div>', unsafe_allow_html=True)
        return

    # Identity summary
    ident = profile.identity
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Core Identity**")
        st.markdown(f"- **Customer ID:** {profile.customer_id}")
        st.markdown(f"- **Customer Name:** {profile.customer_name}")
        st.markdown(f"- **Full Name:** {ident.full_name or 'Unknown'}")
        st.markdown(f"- **Nationality:** {ident.nationality or 'Unknown'}")
        st.markdown(f"- **Residence:** {ident.residence_country or 'Unknown'}")
        st.markdown(f"- **Date of Birth:** {ident.dob or 'Unknown'}")
    with col2:
        st.markdown("**Identifiers**")
        st.markdown(f"- **ID Type:** {ident.id_type or 'Unknown'}")
        st.markdown(f"- **ID Number:** {ident.id_number or 'Unknown'}")
        st.markdown(f"- **Segment:** {ident.risk_segment or 'Unknown'}")
        st.markdown(
            f"- **Existing To Bank (ETB):** {'Yes' if ident.is_etb else 'No/Unknown'}")

    st.markdown("---")
    st.markdown("**AML / KYC Summary (from AML JSON)**")

    # Derive AML summary without dumping the full JSON
    pep_flag = any(getattr(p, 'is_pep', False)
                   for p in (profile.pep_status or []))
    sanctions_flag = bool(profile.sanctions)
    adverse_flag = bool(profile.adverse_news)
    related_count = len(profile.relationships or [])

    col3, col4, col5, col6 = st.columns(4)
    with col3:
        st.metric("PEP", "YES" if pep_flag else "NO")
    with col4:
        st.metric("Sanctions Hits", str(len(profile.sanctions or [])))
    with col5:
        st.metric("Adverse News", str(len(profile.adverse_news or [])))
    with col6:
        st.metric("Connected Persons", str(related_count))

    # If the AML JSON includes a risk score string, surface it
    risk_score_str = None
    try:
        raw = profile.raw_data or {}
        aml = raw.get("aml_json", {})
        if isinstance(aml, str):
            aml = json.loads(aml)
        if isinstance(aml, dict):
            risk_score_str = aml.get("risk_score")
    except Exception:
        risk_score_str = None

    if risk_score_str:
        st.markdown(f"- **AML risk_score:** {risk_score_str}")

    st.markdown('</div>', unsafe_allow_html=True)


def _render_followup_tab():
    """Render follow-up questions tab for requesting additional information."""
    st.markdown('<div class="info-card">', unsafe_allow_html=True)
    st.markdown("### Request Follow-up Information")
    st.caption(
        "Generate additional questions if the assessment needs more information.")

    feedback = st.text_area(
        "What additional information is needed?",
        placeholder="e.g., Need more details about source of wealth from property sales...",
        key="followup_feedback"
    )

    if st.button("🔄 Generate Follow-up Questions", use_container_width=True):
        if feedback:
            with st.spinner("Generating follow-up questions..."):
                try:
                    followup_session_id, followup_q = asyncio.run(
                        st.session_state.coordinator.generate_followup_and_continue(
                            st.session_state.session_id, feedback
                        )
                    )

                    # Save the parent session
                    st.session_state.session_manager.save_checkpoint(
                        st.session_state.session_id,
                        "followup_requested"
                    )

                    # Switch to new follow-up session
                    st.session_state.session_id = followup_session_id
                    st.session_state.questionnaire = followup_q
                    st.session_state.responses = {}
                    st.session_state.ecdd_assessment = None
                    st.session_state.document_checklist = None
                    st.session_state.page = 'questionnaire'

                    st.success(
                        f"Generated {followup_q.get_total_questions()} follow-up questions")
                    st.rerun()

                except Exception as e:
                    st.error(f"Error generating follow-up: {e}")
        else:
            st.warning("Please describe what additional information is needed.")

    # Show if this is a follow-up session
    session = st.session_state.coordinator.get_session(
        st.session_state.session_id)
    if session and session.is_followup:
        st.info(
            f"ℹ️ This is a follow-up session (parent: {session.parent_session_id[:8]}...)")

    st.markdown('</div>', unsafe_allow_html=True)


def _render_history_tab():
    """Render customer ECDD history tab for periodic reviews."""
    st.markdown('<div class="info-card">', unsafe_allow_html=True)
    st.markdown("### Customer ECDD History")
    st.caption(
        "View past assessments for this customer to support periodic reviews.")

    customer_id = st.session_state.questionnaire.customer_id

    # Get history from both sources
    local_history = st.session_state.session_manager.get_customer_history(
        customer_id)

    if local_history:
        st.markdown(
            f"**Found {len(local_history)} assessment(s) for this customer:**")

        for i, session in enumerate(local_history, 1):
            is_current = session.session_id == st.session_state.session_id

            with st.expander(
                f"{'📌 CURRENT - ' if is_current else ''}{session.created_at[:10]} - {session.status.value.replace('_', ' ').title()}",
                expanded=is_current
            ):
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown(
                        f"**Session ID:** `{session.session_id[:8]}...`")
                    st.markdown(f"**Status:** {session.status.value}")
                    st.markdown(f"**Created:** {session.created_at[:16]}")
                    if session.is_followup:
                        st.markdown(
                            f"**Follow-up of:** `{session.parent_session_id[:8]}...`")

                with col2:
                    if session.ecdd_assessment:
                        assessment = session.ecdd_assessment
                        level_emoji = {
                            "low": "🟢", "medium": "🟡", "high": "🔴", "critical": "🔴"
                        }.get(assessment.overall_ecdd_rating.value, "⚪")

                        st.markdown(
                            f"**ECDD Level:** {level_emoji} {assessment.overall_ecdd_rating.value.upper()}")

                        # Show compliance flags
                        flags = assessment.compliance_flags
                        active_flags = []
                        if flags.pep:
                            active_flags.append("PEP")
                        if flags.sanctions:
                            active_flags.append("Sanctions")
                        if flags.adverse_media:
                            active_flags.append("Adverse Media")

                        if active_flags:
                            st.markdown(
                                f"**Flags:** ⚠️ {', '.join(active_flags)}")
                        else:
                            st.markdown("**Flags:** ✓ None")

                # Compare button
                if not is_current and session.ecdd_assessment:
                    if st.button(f"📊 Compare with Current", key=f"compare_{session.session_id[:8]}"):
                        try:
                            comparison = asyncio.run(
                                st.session_state.coordinator.compare_assessments(
                                    st.session_state.session_id, session.session_id
                                )
                            )

                            st.markdown("#### Comparison Results")

                            # ECDD level change
                            if comparison["ecdd_level_change"]["changed"]:
                                st.warning(
                                    f"⚠️ ECDD level changed: {comparison['ecdd_level_change']['previous']} → {comparison['ecdd_level_change']['current']}")
                            else:
                                st.success("✓ ECDD level unchanged")

                            # New concerns
                            if comparison["new_concerns"]:
                                st.error(
                                    f"🆕 New concerns: {', '.join(comparison['new_concerns'])}")

                            # Resolved concerns
                            if comparison["resolved_concerns"]:
                                st.success(
                                    f"✓ Resolved: {', '.join(comparison['resolved_concerns'])}")

                        except Exception as e:
                            st.error(f"Comparison failed: {e}")

                # Load button for non-current sessions
                if not is_current:
                    if st.button(f"📂 Load This Session", key=f"load_{session.session_id[:8]}"):
                        _resume_session(session.session_id)
    else:
        st.info("No previous assessments found for this customer.")

    # Try Databricks for additional history
    st.markdown("---")
    if st.button("🔍 Search Databricks History"):
        try:
            db_history = st.session_state.databricks.get_customer_ecdd_history(
                customer_id)
            if db_history:
                st.success(f"Found {len(db_history)} record(s) in Databricks")
                for record in db_history:
                    with st.expander(f"📦 {record.get('created_at', 'Unknown date')[:10]}"):
                        # Render a readable summary (avoid raw JSON output)
                        st.markdown(
                            f"- **Session ID:** {record.get('session_id', 'N/A')}")
                        st.markdown(
                            f"- **Status:** {record.get('status', 'N/A')}")
                        st.markdown(
                            f"- **Customer:** {record.get('customer_name', 'N/A')} ({record.get('case_id', record.get('customer_id', 'N/A'))})")
                        st.markdown(
                            f"- **Reviewed By:** {record.get('reviewed_by', 'N/A')}")
                        st.markdown(
                            f"- **Review Decision:** {record.get('review_decision', 'N/A')}")

                        flags = record.get('compliance_flags') or {}
                        if isinstance(flags, dict):
                            active = [k for k, v in flags.items() if v]
                            st.markdown(
                                f"- **Compliance Flags:** {'⚠️ ' + ', '.join(active) if active else '✓ None'}")

                        assessment = record.get('ecdd_assessment') or {}
                        if isinstance(assessment, dict):
                            st.markdown(
                                f"- **Client Type:** {assessment.get('client_type', 'N/A')}")
                            st.markdown(
                                f"- **Client Category:** {assessment.get('client_category', 'N/A')}")

                        checklist = record.get('document_checklist') or {}
                        if isinstance(checklist, dict):
                            # Show document counts by category
                            def _count(k: str) -> int:
                                v = checklist.get(k) or []
                                return len(v) if isinstance(v, list) else 0
                            st.markdown(
                                "- **Document Checklist:** "
                                f"ID({_count('identity_documents')}), SoW({_count('source_of_wealth_documents')}), "
                                f"SoF({_count('source_of_funds_documents')}), Compliance({_count('compliance_documents')}), "
                                f"Additional({_count('additional_documents')})"
                            )
            else:
                st.info("No additional records in Databricks")
        except Exception as e:
            st.warning(f"Databricks search unavailable: {e}")

    st.markdown('</div>', unsafe_allow_html=True)


def _render_query_tab():
    """Render stakeholder query tab."""
    st.markdown('<div class="info-card">', unsafe_allow_html=True)
    st.markdown("### Ask Questions About This Assessment")

    query = st.text_area(
        "Enter your question",
        placeholder="e.g., What is the basis for the PEP classification?"
    )

    if st.button("🔍 Get Answer") and query:
        with st.spinner("Analyzing..."):
            try:
                answer = asyncio.run(
                    st.session_state.coordinator.answer_query(
                        st.session_state.session_id, query
                    )
                )
                st.markdown("**Answer:**")
                st.markdown(answer)
            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown('</div>', unsafe_allow_html=True)


def _render_export_tab():
    """Render export options."""
    st.markdown('<div class="info-card">', unsafe_allow_html=True)
    st.markdown("### Export Reports")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("📄 Export ECDD Assessment PDF", use_container_width=True):
            _export_assessment_pdf()

        if st.button("📋 Export Document Checklist PDF", use_container_width=True):
            _export_checklist_pdf()

    with col2:
        if st.button("📝 Export Questionnaire (Filled)", use_container_width=True):
            _export_questionnaire_filled()

        if st.button("📝 Export Questionnaire (Blank)", use_container_width=True):
            _export_questionnaire_blank()

    st.markdown("---")
    if st.button("📦 Export All Documents", use_container_width=True, type="primary"):
        _export_all()

    st.markdown('</div>', unsafe_allow_html=True)


def _export_assessment_pdf():
    """Export ECDD Assessment PDF."""
    with st.spinner("Generating PDF..."):
        try:
            session = st.session_state.coordinator.get_session(
                st.session_state.session_id)
            path = st.session_state.exporter.export_ecdd_assessment(
                st.session_state.ecdd_assessment,
                session,
                save_to_volumes=True
            )
            st.success(f"Exported to: {path}")
        except Exception as e:
            st.error(f"Export failed: {e}")


def _export_checklist_pdf():
    """Export checklist PDF."""
    with st.spinner("Generating PDF..."):
        try:
            session = st.session_state.coordinator.get_session(
                st.session_state.session_id)
            path = st.session_state.exporter.export_document_checklist(
                st.session_state.document_checklist,
                session,
                save_to_volumes=True
            )
            st.success(f"Exported to: {path}")
        except Exception as e:
            st.error(f"Export failed: {e}")


def _export_questionnaire_filled():
    """Export filled questionnaire."""
    with st.spinner("Generating PDF..."):
        try:
            path = st.session_state.exporter.export_questionnaire(
                st.session_state.questionnaire,
                st.session_state.responses
            )
            st.success(f"Exported to: {path}")
        except Exception as e:
            st.error(f"Export failed: {e}")


def _export_questionnaire_blank():
    """Export blank questionnaire."""
    with st.spinner("Generating PDF..."):
        try:
            path = st.session_state.exporter.export_questionnaire(
                st.session_state.questionnaire,
                empty_form=True
            )
            st.success(f"Exported to: {path}")
        except Exception as e:
            st.error(f"Export failed: {e}")


def _export_all():
    """Export all documents."""
    with st.spinner("Generating all documents..."):
        try:
            session = st.session_state.coordinator.get_session(
                st.session_state.session_id)
            paths = st.session_state.exporter.export_all(
                session, save_to_volumes=True)

            st.success("All documents exported:")
            for name, path in paths.items():
                st.markdown(f"- {name}: `{path}`")
        except Exception as e:
            st.error(f"Export failed: {e}")


def _complete_review(decision: str):
    """Complete the review with a decision."""
    try:
        asyncio.run(
            st.session_state.coordinator.complete_review(
                st.session_state.session_id, decision, reviewed_by="Stakeholder"
            )
        )

        st.session_state.session_manager.update_session(
            st.session_state.session_id,
            status=SessionStatus.APPROVED if decision == "approved" else
            SessionStatus.ESCALATED if decision == "escalated" else
            SessionStatus.REJECTED,
            review_decision=decision,
            reviewed_by="Stakeholder",
            save_immediately=True
        )

        # Write to Databricks
        try:
            # ASYNC: get output (if async in coordinator)
            output = asyncio.run(
                st.session_state.coordinator.get_ecdd_output(
                    st.session_state.session_id)
            )
            st.session_state.databricks.write_ecdd_output(output)
            st.success(
                f"Review completed: {decision.upper()} - Saved to Databricks")
        except Exception as e:
            st.warning(f"Saved locally (Databricks unavailable): {e}")

        st.session_state.page = 'home'
        st.rerun()

    except Exception as e:
        st.error(f"Error completing review: {e}")


# =============================================================================
# SIDEBAR
# =============================================================================

def render_sidebar():
    """Render navigation sidebar."""
    with st.sidebar:
        st.markdown("## 📋 ECDD System")
        st.markdown("---")

        # Navigation
        st.markdown("### Navigation")

        if st.button("🏠 Home", use_container_width=True):
            st.session_state.page = 'home'
            st.rerun()

        if st.session_state.session_id:
            st.markdown("---")
            st.markdown("### Current Session")
            st.caption(f"ID: {st.session_state.session_id[:8]}...")

            if st.session_state.client_profile:
                st.caption(
                    f"Client: {st.session_state.client_profile.customer_name}")

        st.markdown("---")
        st.markdown("### System Status")

        status = st.session_state.coordinator.get_status(
        ) if st.session_state.coordinator else {}

        if status.get('coordinator', {}).get('initialized'):
            st.success("✓ Agents Ready")
        else:
            st.warning("⚠ Agents Pending")

        if st.session_state.databricks and st.session_state.databricks.is_connected():
            st.success("✓ Databricks Connected")
        else:
            st.info("○ Databricks Offline")


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main application entry point."""
    load_custom_css()
    init_session_state()
    render_sidebar()

    # Route to current page
    page = st.session_state.page

    if page == 'home':
        page_home()
    elif page == 'questionnaire':
        page_questionnaire()
    elif page == 'review':
        page_review()
    else:
        page_home()


if __name__ == "__main__":
    main()
