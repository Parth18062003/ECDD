"""
Agent 2: Reporter & Validator

Ingests customer answers and generates:
1. Structured checklist (for Delta Table storage)
2. Formal ECDD Assessment report (for Volumes as PDF)

Works in coordination with Agent 1 (Questionnaire Generator) via ECDDAgentCoordinator.

Responsibilities:
1. Analyze questionnaire responses
2. Generate structured compliance flags
3. Create ECDD Assessment with risk ratings
4. Generate document checklists
5. Handle stakeholder queries about the assessment
"""

import os
import json
import uuid
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone

from .schemas import (
    ClientProfile,
    QuestionnaireSession,
    ECDDAssessment,
    DocumentChecklist,
    ComplianceFlags,
    RiskFactor,
    RiskLevel,
    DocumentItem,
    DocumentPriority,
)

logger = logging.getLogger(__name__)

# Azure AI Configuration
PROJECT_ENDPOINT = os.environ.get(
    "AZURE_AI_PROJECT_ENDPOINT",
    "https://aistudiosz-canadaeast.services.ai.azure.com/api/projects/agentic-ai-demo-sz"
)
MODEL_DEPLOYMENT = os.environ.get("AZURE_MODEL_DEPLOYMENT", "gpt-4o")
REPORTER_AGENT_NAME = "ecdd-reporter-validator"


# =============================================================================
# REPORT GENERATION PROMPTS
# =============================================================================

REPORTER_SYSTEM_PROMPT = """You are an ECDD (Enhanced Client Due Diligence) Assessment Specialist for a major bank.

Your role is to analyze questionnaire responses and generate:
1. A formal ECDD Assessment Report
2. A structured document checklist
3. Compliance flags for automated processing

You must be thorough, professional, and risk-aware. Your assessments affect regulatory compliance.

ECDD ASSESSMENT REPORT FORMAT:
Generate a professional report with these sections:
1. CLIENT IDENTIFICATION
2. CLIENT TYPE CLASSIFICATION  
3. SOURCE OF WEALTH VERIFICATION
4. SOURCE OF FUNDS ANALYSIS
5. RISK FACTOR ASSESSMENT (with ratings: low/medium/high/critical)
6. COMPLIANCE FLAGS
7. OVERALL RISK RATING
8. ADVISOR RECOMMENDATIONS

STRUCTURED OUTPUT FORMAT (return this JSON block at the END of your response):
```json
{
    "client_type": "High Net Worth Individual | Corporate | Trust | ...",
    "client_category": "Individual | Corporate | Trust | ...",
    "overall_risk_rating": "low | medium | high | critical",
    "risk_score": 0.0 to 1.0,
    "risk_factors": [
        {"factor_name": "PEP Status", "level": "high", "score": 0.8, "justification": "..."}
    ],
    "compliance_flags": {
        "pep": true/false,
        "sanctions": true/false,
        "adverse_media": true/false,
        "high_risk_jurisdiction": true/false,
        "watchlist_hit": true/false,
        "source_of_wealth_concerns": true/false,
        "source_of_funds_concerns": true/false,
        "complex_ownership": true/false
    },
    "recommendations": ["action 1", "action 2", ...],
    "required_actions": ["action 1", ...]
}
```

DOCUMENT CHECKLIST FORMAT (return this JSON block after the structured output):
```documents
{
    "identity_documents": [
        {"document_name": "...", "priority": "required|recommended|optional", "category": "identity", "special_instructions": "..."}
    ],
    "source_of_wealth_documents": [...],
    "source_of_funds_documents": [...],
    "compliance_documents": [...],
    "additional_documents": [...]
}
```
"""

STAKEHOLDER_QUERY_PROMPT = """You are an ECDD Assessment Specialist answering questions about an assessment.

You have access to:
1. The client profile
2. The questionnaire responses
3. The generated ECDD Assessment

Answer the stakeholder's question professionally and thoroughly.
Reference specific data from the assessment when relevant.
"""


class ReporterValidatorAgent:
    """
    Agent 2: Generates ECDD Assessments and Document Checklists.
    
    Processes questionnaire responses to create:
    - Structured data for Delta Table storage
    - Formal reports for PDF export to Volumes
    """
    
    def __init__(self, project_endpoint: str = PROJECT_ENDPOINT):
        self.project_endpoint = project_endpoint
        self._client = None
        self._agent = None
        self._initialized = False
        self._threads: Dict[str, str] = {}  # session_id -> thread_id for context
    
    def initialize(self) -> bool:
        """Initialize Azure AI client and agent."""
        if self._initialized:
            return True
        
        try:
            from azure.ai.projects import AIProjectClient
            from azure.identity import DefaultAzureCredential
            
            self._client = AIProjectClient(
                endpoint=self.project_endpoint,
                credential=DefaultAzureCredential()
            )
            
            self._agent = self._get_or_create_agent()
            self._initialized = True
            logger.info("ReporterValidatorAgent initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize ReporterValidatorAgent: {e}")
            return False
    
    def _get_or_create_agent(self):
        """Get existing agent or create a new one."""
        try:
            agents = self._client.agents.list_agents()
            for agent in agents.data:
                if agent.name == REPORTER_AGENT_NAME:
                    logger.info(f"Found existing reporter agent: {agent.id}")
                    return agent
        except Exception as e:
            logger.warning(f"Could not list agents: {e}")
        
        agent = self._client.agents.create_agent(
            model=MODEL_DEPLOYMENT,
            name=REPORTER_AGENT_NAME,
            instructions=REPORTER_SYSTEM_PROMPT,
            temperature=0.2  # Low temperature for consistent structured output
        )
        logger.info(f"Created new reporter agent: {agent.id}")
        return agent
    
    def generate_assessment(
        self,
        client_profile: ClientProfile,
        questionnaire_responses: Dict[str, Any],
        session_id: str
    ) -> tuple[ECDDAssessment, DocumentChecklist]:
        """
        Generate ECDD Assessment and Document Checklist from responses.
        
        Args:
            client_profile: The client's profile data
            questionnaire_responses: Answers to questionnaire questions
            session_id: Session ID for thread management
            
        Returns:
            Tuple of (ECDDAssessment, DocumentChecklist)
        """
        if not self._initialized:
            self.initialize()
        
        # Build comprehensive prompt
        profile_summary = client_profile.get_summary_for_agent()
        responses_text = self._format_responses(questionnaire_responses)
        
        prompt = f"""Generate a formal ECDD Assessment Report based on the following:

CLIENT PROFILE:
{profile_summary}

QUESTIONNAIRE RESPONSES:
{responses_text}

ADDITIONAL CONTEXT:
- PEP Hits: {len(client_profile.pep_status)}
- Sanctions Hits: {len(client_profile.sanctions)}
- Adverse News Items: {len(client_profile.adverse_news)}
- Related Parties: {len(client_profile.relationships)}

Please generate:
1. A formal ECDD Assessment Report (professional narrative format)
2. The structured JSON output block at the end
3. The document checklist JSON block

Be thorough and identify all relevant risk factors."""

        try:
            # Create or get thread for this session
            if session_id in self._threads:
                thread_id = self._threads[session_id]
            else:
                thread = self._client.agents.create_thread()
                thread_id = thread.id
                self._threads[session_id] = thread_id
            
            self._client.agents.create_message(
                thread_id=thread_id,
                role="user",
                content=prompt
            )
            
            run = self._client.agents.create_and_process_run(
                thread_id=thread_id,
                agent_id=self._agent.id
            )
            
            # Get response
            messages = self._client.agents.list_messages(thread_id=thread_id)
            response_text = ""
            for msg in messages.data:
                if msg.role == "assistant":
                    for content in msg.content:
                        if hasattr(content, 'text'):
                            response_text = content.text.value
                            break
            
            # Parse the response
            return self._parse_assessment_response(response_text, client_profile)
            
        except Exception as e:
            logger.error(f"Error generating assessment: {e}")
            return self._generate_fallback_assessment(client_profile)
    
    def answer_stakeholder_query(
        self,
        session_id: str,
        question: str,
        client_profile: ClientProfile,
        assessment: ECDDAssessment
    ) -> str:
        """
        Answer a stakeholder's question about the assessment.
        
        Args:
            session_id: Session ID for thread context
            question: Stakeholder's question
            client_profile: Client profile
            assessment: The generated assessment
            
        Returns:
            Answer as string
        """
        if not self._initialized:
            self.initialize()
        
        context = f"""CONTEXT FOR ANSWERING:
Client: {client_profile.customer_name} (ID: {client_profile.customer_id})
Overall Risk Rating: {assessment.overall_risk_rating.value}
Client Type: {assessment.client_type}
Key Compliance Flags: {assessment.compliance_flags.model_dump_json()}

STAKEHOLDER QUESTION:
{question}

Please provide a thorough, professional answer based on the assessment data."""

        try:
            # Use existing thread if available for context continuity
            if session_id in self._threads:
                thread_id = self._threads[session_id]
            else:
                thread = self._client.agents.create_thread()
                thread_id = thread.id
                self._threads[session_id] = thread_id
            
            self._client.agents.create_message(
                thread_id=thread_id,
                role="user",
                content=STAKEHOLDER_QUERY_PROMPT + "\n\n" + context
            )
            
            run = self._client.agents.create_and_process_run(
                thread_id=thread_id,
                agent_id=self._agent.id
            )
            
            messages = self._client.agents.list_messages(thread_id=thread_id)
            for msg in messages.data:
                if msg.role == "assistant":
                    for content in msg.content:
                        if hasattr(content, 'text'):
                            return content.text.value
            
            return "I apologize, but I couldn't generate a response. Please try again."
            
        except Exception as e:
            logger.error(f"Error answering stakeholder query: {e}")
            return f"Error processing query: {str(e)}"
    
    def _format_responses(self, responses: Dict[str, Any]) -> str:
        """Format questionnaire responses for the prompt."""
        lines = []
        for field_id, value in responses.items():
            # Clean up field ID to be more readable
            readable_id = field_id.replace("_", " ").title()
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value)
            lines.append(f"- {readable_id}: {value}")
        return "\n".join(lines) if lines else "No responses provided."
    
    def _parse_assessment_response(
        self, 
        response: str,
        client_profile: ClientProfile
    ) -> tuple[ECDDAssessment, DocumentChecklist]:
        """Parse AI response into structured objects."""
        
        # Extract the full report text (everything before JSON blocks)
        report_text = response
        
        # Try to extract structured JSON
        assessment = ECDDAssessment(report_text=report_text)
        checklist = DocumentChecklist(checklist_text=response)
        
        try:
            # Extract assessment JSON block
            if "```json" in response:
                json_blocks = response.split("```json")
                for block in json_blocks[1:]:
                    json_text = block.split("```")[0].strip()
                    try:
                        data = json.loads(json_text)
                        
                        # Parse if it looks like assessment data
                        if "overall_risk_rating" in data or "compliance_flags" in data:
                            assessment.client_type = data.get("client_type", "")
                            assessment.client_category = data.get("client_category", "")
                            assessment.overall_risk_rating = RiskLevel(
                                data.get("overall_risk_rating", "medium")
                            )
                            assessment.risk_score = float(data.get("risk_score", 0.5))
                            
                            # Parse risk factors
                            for rf in data.get("risk_factors", []):
                                assessment.risk_factors.append(RiskFactor(
                                    factor_name=rf.get("factor_name", ""),
                                    level=RiskLevel(rf.get("level", "medium")),
                                    score=float(rf.get("score", 0.5)),
                                    justification=rf.get("justification", "")
                                ))
                            
                            # Parse compliance flags
                            flags_data = data.get("compliance_flags", {})
                            assessment.compliance_flags = ComplianceFlags(
                                pep=flags_data.get("pep", False),
                                sanctions=flags_data.get("sanctions", False),
                                adverse_media=flags_data.get("adverse_media", False),
                                high_risk_jurisdiction=flags_data.get("high_risk_jurisdiction", False),
                                watchlist_hit=flags_data.get("watchlist_hit", False),
                                source_of_wealth_concerns=flags_data.get("source_of_wealth_concerns", False),
                                source_of_funds_concerns=flags_data.get("source_of_funds_concerns", False),
                                complex_ownership=flags_data.get("complex_ownership", False)
                            )
                            
                            assessment.recommendations = data.get("recommendations", [])
                            assessment.required_actions = data.get("required_actions", [])
                    except json.JSONDecodeError:
                        continue
            
            # Extract document checklist JSON block  
            if "```documents" in response:
                doc_block = response.split("```documents")[1].split("```")[0].strip()
                try:
                    doc_data = json.loads(doc_block)
                    checklist = self._parse_document_checklist(doc_data)
                    checklist.checklist_text = response
                except json.JSONDecodeError:
                    pass
            
        except Exception as e:
            logger.warning(f"Error parsing structured data: {e}")
        
        # Infer compliance flags from profile if not set
        if not any([assessment.compliance_flags.pep, assessment.compliance_flags.sanctions]):
            if client_profile.pep_status and any(p.is_pep for p in client_profile.pep_status):
                assessment.compliance_flags.pep = True
            if client_profile.sanctions:
                assessment.compliance_flags.sanctions = True
            if client_profile.adverse_news:
                assessment.compliance_flags.adverse_media = True
        
        return assessment, checklist
    
    def _parse_document_checklist(self, data: Dict) -> DocumentChecklist:
        """Parse document checklist JSON into structured object."""
        def parse_docs(docs_list: List[Dict]) -> List[DocumentItem]:
            items = []
            for d in docs_list:
                try:
                    items.append(DocumentItem(
                        document_name=d.get("document_name", ""),
                        priority=DocumentPriority(d.get("priority", "required")),
                        category=d.get("category", ""),
                        acceptable_formats=d.get("acceptable_formats", []),
                        special_instructions=d.get("special_instructions", "")
                    ))
                except:
                    continue
            return items
        
        return DocumentChecklist(
            identity_documents=parse_docs(data.get("identity_documents", [])),
            source_of_wealth_documents=parse_docs(data.get("source_of_wealth_documents", [])),
            source_of_funds_documents=parse_docs(data.get("source_of_funds_documents", [])),
            compliance_documents=parse_docs(data.get("compliance_documents", [])),
            additional_documents=parse_docs(data.get("additional_documents", []))
        )
    
    def _generate_fallback_assessment(
        self, 
        client_profile: ClientProfile
    ) -> tuple[ECDDAssessment, DocumentChecklist]:
        """Generate fallback assessment if AI generation fails."""
        
        # Infer risk from profile
        risk_level = RiskLevel.MEDIUM
        risk_factors = []
        
        if client_profile.pep_status and any(p.is_pep for p in client_profile.pep_status):
            risk_level = RiskLevel.HIGH
            risk_factors.append(RiskFactor(
                factor_name="PEP Status",
                level=RiskLevel.HIGH,
                score=0.8,
                justification="Client identified as Politically Exposed Person"
            ))
        
        if client_profile.sanctions:
            risk_level = RiskLevel.CRITICAL
            risk_factors.append(RiskFactor(
                factor_name="Sanctions Screening",
                level=RiskLevel.CRITICAL,
                score=0.95,
                justification=f"Sanctions screening returned {len(client_profile.sanctions)} hit(s)"
            ))
        
        if client_profile.adverse_news:
            if risk_level == RiskLevel.MEDIUM:
                risk_level = RiskLevel.HIGH
            risk_factors.append(RiskFactor(
                factor_name="Adverse Media",
                level=RiskLevel.HIGH,
                score=0.7,
                justification=f"Adverse news screening found {len(client_profile.adverse_news)} item(s)"
            ))
        
        assessment = ECDDAssessment(
            client_type="Unknown (Fallback Assessment)",
            client_category="Individual",
            overall_risk_rating=risk_level,
            risk_score=0.5,
            risk_factors=risk_factors,
            compliance_flags=ComplianceFlags(
                pep=bool(client_profile.pep_status and any(p.is_pep for p in client_profile.pep_status)),
                sanctions=bool(client_profile.sanctions),
                adverse_media=bool(client_profile.adverse_news)
            ),
            recommendations=[
                "Complete manual review of client documentation",
                "Verify source of wealth documentation",
                "Obtain enhanced due diligence sign-off"
            ],
            report_text="FALLBACK ASSESSMENT - AI generation failed. Please complete manual review."
        )
        
        checklist = DocumentChecklist(
            identity_documents=[
                DocumentItem(
                    document_name="Government-issued photo ID",
                    priority=DocumentPriority.REQUIRED,
                    category="identity"
                ),
                DocumentItem(
                    document_name="Proof of address (utility bill, bank statement)",
                    priority=DocumentPriority.REQUIRED,
                    category="identity"
                ),
            ],
            source_of_wealth_documents=[
                DocumentItem(
                    document_name="Evidence of source of wealth",
                    priority=DocumentPriority.REQUIRED,
                    category="sow"
                ),
            ],
            source_of_funds_documents=[
                DocumentItem(
                    document_name="Bank statements (3 months)",
                    priority=DocumentPriority.REQUIRED,
                    category="sof"
                ),
            ],
            checklist_text="Standard document checklist - customize based on client profile."
        )
        
        return assessment, checklist
    
    def get_status(self) -> Dict[str, Any]:
        """Get agent status."""
        return {
            "name": "ReporterValidatorAgent",
            "initialized": self._initialized,
            "agent_id": self._agent.id if self._agent else None,
            "active_threads": len(self._threads),
            "endpoint": self.project_endpoint
        }
