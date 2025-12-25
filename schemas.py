"""
ECDD Schemas - Flexible data models for Enhanced Client Due Diligence

Design Principles:
1. Client profile structure is easily configurable via ProfileConfig
2. All models use Pydantic for validation and JSON serialization
3. Nested JSON structures optimized for Delta Table storage
4. No orchestrator dependencies
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field
from dataclasses import dataclass, field


# =============================================================================
# CONFIGURATION - Easily modify when schema changes
# =============================================================================

@dataclass
class ProfileFieldConfig:
    """Configuration for a single profile field."""
    name: str
    source_keys: List[str]  # Multiple possible keys in source data
    required: bool = False
    default: Any = None


@dataclass  
class ProfileConfig:
    """
    Configuration for client profile structure.
    Modify this to easily change which fields are extracted from source data.
    """
    # Identity fields - add/remove/modify as needed
    identity_fields: List[ProfileFieldConfig] = field(default_factory=lambda: [
        ProfileFieldConfig("customer_id", ["case_id", "customer_id", "client_id"], required=True),
        ProfileFieldConfig("full_name", ["customer_name", "full_name", "fullName", "name"], required=True),
        ProfileFieldConfig("nationality", ["nationality", "country_of_citizenship"]),
        ProfileFieldConfig("residence_country", ["residence_country", "country_of_residence", "residence"]),
        ProfileFieldConfig("dob", ["dob", "date_of_birth", "birth_date"]),
        ProfileFieldConfig("id_type", ["id_type", "document_type", "identification_type"]),
        ProfileFieldConfig("id_number", ["id_number", "document_number", "identification_number"]),
        ProfileFieldConfig("known_aliases", ["known_aliases", "aliases", "aka"]),
        ProfileFieldConfig("risk_segment", ["risk_segment", "client_segment", "segment"]),
        ProfileFieldConfig("is_etb", ["is_etb", "existing_to_bank", "etb_flag"]),
    ])
    
    # Nested data sections - keys to look for in aml_json
    pep_keys: List[str] = field(default_factory=lambda: [
        "pep_screening_status", "pepScreeningStatus", "pep_status", "pep", "pep_screening"
    ])
    sanctions_keys: List[str] = field(default_factory=lambda: [
        "sanctions", "sanctionsList", "sanctions_screening", "sanction_hits"
    ])
    adverse_news_keys: List[str] = field(default_factory=lambda: [
        "risk_adverse_news", "adverse_news", "adverseNews", "adverse_media", "negative_news"
    ])
    relationships_keys: List[str] = field(default_factory=lambda: [
        "relationships", "relations", "connections", "related_parties"
    ])
    accounts_keys: List[str] = field(default_factory=lambda: [
        "accounts", "bank_accounts", "accountList"
    ])
    watchlist_keys: List[str] = field(default_factory=lambda: [
        "warrants_watchlists", "watchlists", "warrants", "watchlist_hits"
    ])


# Global default config - modify or create new instances as needed
DEFAULT_PROFILE_CONFIG = ProfileConfig()


# =============================================================================
# ENUMS
# =============================================================================

class SessionStatus(str, Enum):
    """Session lifecycle states."""
    PENDING = "pending"
    QUESTIONNAIRE_GENERATED = "questionnaire_generated"
    IN_PROGRESS = "in_progress"
    RESPONSES_SUBMITTED = "responses_submitted"
    REPORTS_GENERATED = "reports_generated"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    ESCALATED = "escalated"
    REJECTED = "rejected"
    ERROR = "error"


class RiskLevel(str, Enum):
    """Risk classification levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DocumentPriority(str, Enum):
    """Document requirement priority."""
    REQUIRED = "required"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"


class QuestionType(str, Enum):
    """Question field types for questionnaires."""
    TEXT = "text"
    TEXTAREA = "textarea"
    DROPDOWN = "dropdown"
    MULTIPLE_CHOICE = "multiple_choice"
    CHECKBOX = "checkbox"
    DATE = "date"
    NUMBER = "number"
    CURRENCY = "currency"
    YES_NO = "yes_no"


# =============================================================================
# CLIENT PROFILE MODELS (Input - Flexible Structure)
# =============================================================================

class IdentityProfile(BaseModel):
    """Client identity information - fields are optional for flexibility."""
    customer_id: Optional[str] = None
    full_name: Optional[str] = None
    known_aliases: Optional[str] = None
    dob: Optional[str] = None
    nationality: Optional[str] = None
    residence_country: Optional[str] = None
    id_type: Optional[str] = None
    id_number: Optional[str] = None
    is_etb: Optional[bool] = None
    risk_segment: Optional[str] = None
    
    class Config:
        extra = "allow"  # Allow additional fields not defined here


class PEPStatus(BaseModel):
    """Politically Exposed Person screening result."""
    is_pep: bool = False
    pep_type: Optional[str] = None
    position_title: Optional[str] = None
    country: Optional[str] = None
    relationship_type: Optional[str] = None  # direct, family, associate
    last_verified: Optional[str] = None
    notes: Optional[str] = None
    
    class Config:
        extra = "allow"


class SanctionHit(BaseModel):
    """Sanctions screening result."""
    list_name: Optional[str] = None  # OFAC, UN, EU, etc.
    match_type: Optional[str] = None
    match_confidence: Optional[float] = None
    program: Optional[str] = None
    notes: Optional[str] = None
    
    class Config:
        extra = "allow"


class AdverseNewsItem(BaseModel):
    """Adverse media/news item."""
    source_name: Optional[str] = None
    source_url: Optional[str] = None
    publication_date: Optional[str] = None
    headline: Optional[str] = None
    summary: Optional[str] = None
    category: Optional[str] = None  # fraud, corruption, crime, etc.
    sentiment: Optional[str] = None
    risk_level: Optional[str] = None
    
    class Config:
        extra = "allow"


class Relationship(BaseModel):
    """Related party relationship."""
    name: Optional[str] = None
    relationship_type: Optional[str] = None
    connected_id: Optional[str] = None
    notes: Optional[str] = None
    
    class Config:
        extra = "allow"


class Account(BaseModel):
    """Bank account information."""
    account_id: Optional[str] = None
    account_type: Optional[str] = None
    status: Optional[str] = None
    opened_date: Optional[str] = None
    
    class Config:
        extra = "allow"


class ClientProfile(BaseModel):
    """
    Complete client profile for ECDD assessment.
    
    This model is flexible - fields can be easily added/removed via ProfileConfig.
    All nested lists accept any structure for maximum compatibility.
    """
    customer_id: str
    customer_name: str
    
    # Core identity
    identity: IdentityProfile = Field(default_factory=IdentityProfile)
    
    # AML screening results
    pep_status: List[PEPStatus] = Field(default_factory=list)
    sanctions: List[SanctionHit] = Field(default_factory=list)
    adverse_news: List[AdverseNewsItem] = Field(default_factory=list)
    watchlist_hits: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Relationships and accounts
    relationships: List[Relationship] = Field(default_factory=list)
    accounts: List[Account] = Field(default_factory=list)
    
    # Source metadata
    document_type: Optional[str] = None
    risk_score: Optional[float] = None
    created_at: Optional[str] = None
    
    # Raw data for anything not mapped above
    raw_data: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        extra = "allow"
    
    @classmethod
    def from_databricks_row(
        cls, 
        row: Dict[str, Any], 
        config: ProfileConfig = None
    ) -> "ClientProfile":
        """
        Create ClientProfile from a Databricks delta table row.
        Uses ProfileConfig to handle various source field names.
        """
        config = config or DEFAULT_PROFILE_CONFIG
        
        def get_value(data: Dict, keys: List[str], default=None):
            """Get value from dict trying multiple possible keys."""
            for key in keys:
                if key in data:
                    return data[key]
                # Try lowercase
                if key.lower() in data:
                    return data[key.lower()]
            return default
        
        def ensure_list(val):
            """Ensure value is a list."""
            if val is None:
                return []
            if isinstance(val, dict):
                return [val]
            if isinstance(val, list):
                return val
            return []
        
        # Parse aml_json column
        import json
        aml_data = row.get("aml_json", {})
        if isinstance(aml_data, str):
            try:
                aml_data = json.loads(aml_data)
            except:
                aml_data = {}
        if not isinstance(aml_data, dict):
            aml_data = {}
        
        # Extract identity fields using config
        identity_dict = {}
        for field_cfg in config.identity_fields:
            val = get_value(row, field_cfg.source_keys) or get_value(aml_data, field_cfg.source_keys)
            if val is not None:
                identity_dict[field_cfg.name] = val
            elif field_cfg.default is not None:
                identity_dict[field_cfg.name] = field_cfg.default
        
        # Get customer_id and customer_name
        customer_id = str(get_value(row, ["case_id", "customer_id"]) or identity_dict.get("customer_id", ""))
        customer_name = str(get_value(row, ["customer_name"]) or identity_dict.get("full_name", ""))
        
        # Parse nested sections
        pep_data = ensure_list(get_value(aml_data, config.pep_keys))
        sanctions_data = ensure_list(get_value(aml_data, config.sanctions_keys))
        adverse_data = ensure_list(get_value(aml_data, config.adverse_news_keys))
        relationships_data = ensure_list(get_value(aml_data, config.relationships_keys))
        accounts_data = ensure_list(get_value(aml_data, config.accounts_keys))
        watchlist_data = ensure_list(get_value(aml_data, config.watchlist_keys))
        
        return cls(
            customer_id=customer_id,
            customer_name=customer_name,
            identity=IdentityProfile(**identity_dict),
            pep_status=[PEPStatus(**p) if isinstance(p, dict) else PEPStatus() for p in pep_data],
            sanctions=[SanctionHit(**s) if isinstance(s, dict) else SanctionHit() for s in sanctions_data],
            adverse_news=[AdverseNewsItem(**a) if isinstance(a, dict) else AdverseNewsItem() for a in adverse_data],
            watchlist_hits=watchlist_data,
            relationships=[Relationship(**r) if isinstance(r, dict) else Relationship() for r in relationships_data],
            accounts=[Account(**a) if isinstance(a, dict) else Account() for a in accounts_data],
            document_type=row.get("document_type"),
            risk_score=row.get("risk_score"),
            created_at=str(row.get("created_at", "")),
            raw_data=row
        )
    
    def get_summary_for_agent(self) -> str:
        """Generate a text summary for the AI agent to analyze."""
        lines = [
            f"Customer: {self.customer_name} (ID: {self.customer_id})",
            f"Nationality: {self.identity.nationality or 'Unknown'}",
            f"Residence: {self.identity.residence_country or 'Unknown'}",
            f"Risk Segment: {self.identity.risk_segment or 'Unknown'}",
            f"Existing Customer: {'Yes' if self.identity.is_etb else 'No/Unknown'}",
        ]
        
        if self.pep_status:
            pep = self.pep_status[0]
            lines.append(f"PEP Status: {'Yes' if pep.is_pep else 'No'}" + 
                        (f" - {pep.pep_type}" if pep.pep_type else ""))
        
        if self.sanctions:
            lines.append(f"Sanctions Hits: {len(self.sanctions)}")
        
        if self.adverse_news:
            lines.append(f"Adverse News Items: {len(self.adverse_news)}")
        
        if self.relationships:
            lines.append(f"Related Parties: {len(self.relationships)}")
        
        return "\n".join(lines)


# =============================================================================
# QUESTIONNAIRE MODELS (Agent 1 Output)
# =============================================================================

class QuestionField(BaseModel):
    """A single question in the questionnaire."""
    field_id: str
    question_text: str
    question_type: QuestionType = QuestionType.TEXT
    required: bool = True
    help_text: str = ""
    options: List[str] = Field(default_factory=list)
    placeholder: str = ""
    category: str = ""
    aml_relevant: bool = False


class QuestionSection(BaseModel):
    """A section of related questions."""
    section_id: str
    section_title: str
    section_description: str = ""
    section_icon: str = "📋"
    order: int = 0
    questions: List[QuestionField] = Field(default_factory=list)


class DynamicQuestionnaire(BaseModel):
    """Complete dynamically generated questionnaire from Agent 1."""
    questionnaire_id: str
    customer_id: str
    customer_name: str
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    sections: List[QuestionSection] = Field(default_factory=list)
    client_type: str = ""
    profile_summary: Dict[str, Any] = Field(default_factory=dict)
    
    def get_total_questions(self) -> int:
        return sum(len(s.questions) for s in self.sections)


# =============================================================================
# ECDD ASSESSMENT MODELS (Agent 2 Output - Structured for Delta Table)
# =============================================================================

class ComplianceFlags(BaseModel):
    """
    Structured compliance flags for Delta Table storage.
    Stored as nested JSON in the compliance_flags column.
    """
    pep: bool = False
    sanctions: bool = False
    adverse_media: bool = False
    high_risk_jurisdiction: bool = False
    watchlist_hit: bool = False
    source_of_wealth_concerns: bool = False
    source_of_funds_concerns: bool = False
    complex_ownership: bool = False
    
    class Config:
        extra = "allow"


class RiskFactor(BaseModel):
    """Individual risk factor in the ECDD Assessment."""
    factor_name: str
    level: RiskLevel = RiskLevel.MEDIUM
    score: float = 0.0
    justification: str = ""


class ECDDAssessment(BaseModel):
    """
    Structured ECDD Assessment (replaces Risk Assessment).
    Designed for Delta Table storage with nested JSON.
    """
    # Client classification
    client_type: str = ""
    client_category: str = ""  # Individual, Corporate, Trust, etc.
    
    # Risk assessment
    overall_risk_rating: RiskLevel = RiskLevel.MEDIUM
    risk_score: float = 0.0
    risk_factors: List[RiskFactor] = Field(default_factory=list)
    
    # Source of wealth/funds
    source_of_wealth: Dict[str, Any] = Field(default_factory=dict)
    source_of_funds: Dict[str, Any] = Field(default_factory=dict)
    
    # Compliance
    compliance_flags: ComplianceFlags = Field(default_factory=ComplianceFlags)
    
    # Recommendations
    recommendations: List[str] = Field(default_factory=list)
    required_actions: List[str] = Field(default_factory=list)
    
    # Metadata
    assessed_by: str = "ECDD Agent"
    assessed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    # Raw report text for PDF generation
    report_text: str = ""


class DocumentItem(BaseModel):
    """Individual document in the checklist."""
    document_name: str
    priority: DocumentPriority = DocumentPriority.REQUIRED
    category: str = ""
    acceptable_formats: List[str] = Field(default_factory=list)
    special_instructions: str = ""
    status: str = "pending"  # pending, received, verified, rejected


class DocumentChecklist(BaseModel):
    """
    Structured document checklist (Agent 2 Output).
    Designed for Delta Table storage with nested JSON.
    """
    identity_documents: List[DocumentItem] = Field(default_factory=list)
    source_of_wealth_documents: List[DocumentItem] = Field(default_factory=list)
    source_of_funds_documents: List[DocumentItem] = Field(default_factory=list)
    compliance_documents: List[DocumentItem] = Field(default_factory=list)
    additional_documents: List[DocumentItem] = Field(default_factory=list)
    
    # Raw text for PDF generation
    checklist_text: str = ""
    
    def get_all_required(self) -> List[DocumentItem]:
        """Get all required documents across categories."""
        all_docs = (
            self.identity_documents + 
            self.source_of_wealth_documents + 
            self.source_of_funds_documents + 
            self.compliance_documents +
            self.additional_documents
        )
        return [d for d in all_docs if d.priority == DocumentPriority.REQUIRED]


# =============================================================================
# SESSION AND OUTPUT MODELS
# =============================================================================

class QuestionnaireSession(BaseModel):
    """Session for questionnaire-based ECDD assessment."""
    session_id: str
    customer_id: str
    customer_name: str
    status: SessionStatus = SessionStatus.PENDING
    
    # Agent threading
    questionnaire_thread_id: Optional[str] = None
    reporter_thread_id: Optional[str] = None
    
    # Timestamps
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    # Data
    client_profile: Dict[str, Any] = Field(default_factory=dict)
    questionnaire: Optional[DynamicQuestionnaire] = None
    responses: Dict[str, Any] = Field(default_factory=dict)
    
    # Results
    ecdd_assessment: Optional[ECDDAssessment] = None
    document_checklist: Optional[DocumentChecklist] = None
    
    # Review
    review_decision: Optional[str] = None
    review_notes: Optional[str] = None
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[str] = None
    
    # Follow-up
    is_followup: bool = False
    parent_session_id: Optional[str] = None


class ECDDOutput(BaseModel):
    """
    Complete ECDD output for Delta Table persistence.
    All nested structures are JSON-serializable for structured storage.
    """
    session_id: str
    customer_id: str
    customer_name: str
    
    # Structured outputs (stored as nested JSON columns)
    compliance_flags: ComplianceFlags = Field(default_factory=ComplianceFlags)
    ecdd_assessment: ECDDAssessment = Field(default_factory=ECDDAssessment)
    document_checklist: DocumentChecklist = Field(default_factory=DocumentChecklist)
    questionnaire_responses: Dict[str, Any] = Field(default_factory=dict)
    
    # Status and timestamps
    status: str = "completed"
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    # Review information
    review_decision: Optional[str] = None
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[str] = None
    
    def to_delta_row(self) -> Dict[str, Any]:
        """Convert to dict suitable for Delta Table insertion."""
        return {
            "session_id": self.session_id,
            "customer_id": self.customer_id,
            "customer_name": self.customer_name,
            "compliance_flags": self.compliance_flags.model_dump_json(),
            "ecdd_assessment": self.ecdd_assessment.model_dump_json(),
            "document_checklist": self.document_checklist.model_dump_json(),
            "questionnaire_responses": json.dumps(self.questionnaire_responses),
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "review_decision": self.review_decision,
            "reviewed_by": self.reviewed_by,
            "reviewed_at": self.reviewed_at,
        }


# Need to import json for the to_delta_row method
import json
