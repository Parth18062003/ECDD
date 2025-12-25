"""
Agent 1: Questionnaire Generator

Analyzes customer context and generates a structured list of ECDD questions.
Works in coordination with Agent 2 (Reporter) via the ECDDAgentCoordinator.

Responsibilities:
1. Analyze client profile to determine risk factors and gaps
2. Generate targeted questionnaire sections
3. Skip questions where data is already known
4. Support follow-up questionnaire generation from stakeholder feedback
"""

import os
import json
import uuid
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone

from .schemas import (
    ClientProfile,
    DynamicQuestionnaire,
    QuestionSection,
    QuestionField,
    QuestionType,
)

logger = logging.getLogger(__name__)

# Azure AI Configuration
PROJECT_ENDPOINT = os.environ.get(
    "AZURE_AI_PROJECT_ENDPOINT",
    "https://aistudiosz-canadaeast.services.ai.azure.com/api/projects/agentic-ai-demo-sz"
)
MODEL_DEPLOYMENT = os.environ.get("AZURE_MODEL_DEPLOYMENT", "gpt-4o")
QUESTIONNAIRE_AGENT_NAME = "ecdd-questionnaire-generator"


# =============================================================================
# QUESTIONNAIRE GENERATION PROMPTS
# =============================================================================

QUESTIONNAIRE_SYSTEM_PROMPT = """You are an ECDD (Enhanced Client Due Diligence) Questionnaire Generator for a major bank.

Your role is to analyze client profiles and generate targeted, risk-focused questions that:
1. Fill gaps in the client's profile information
2. Probe areas of potential concern (PEP status, sanctions, adverse media)
3. Verify source of wealth and source of funds
4. Assess business relationships and transaction patterns

IMPORTANT RULES:
- Skip questions for information already provided in the profile
- Focus on HIGH-RISK areas first (PEP, sanctions, adverse news)
- Generate questions appropriate to the client type (individual vs corporate)
- Use clear, professional banking language
- Each question should have a specific AML/compliance purpose

OUTPUT FORMAT: Return ONLY valid JSON in this exact structure:
{
    "client_type": "High Net Worth Individual | Corporate | Trust | ...",
    "sections": [
        {
            "section_id": "unique_id",
            "section_title": "Section Name",
            "section_description": "Why these questions are needed",
            "section_icon": "📋",
            "order": 1,
            "questions": [
                {
                    "field_id": "unique_field_id",
                    "question_text": "The question to ask",
                    "question_type": "text|textarea|dropdown|multiple_choice|checkbox|date|number|currency|yes_no",
                    "required": true,
                    "help_text": "Guidance for the user",
                    "options": ["option1", "option2"],
                    "category": "identity|sow|sof|business|compliance",
                    "aml_relevant": true
                }
            ]
        }
    ]
}
"""

FOLLOWUP_SYSTEM_PROMPT = """You are generating FOLLOW-UP questions for an ECDD assessment based on stakeholder feedback.

The stakeholder has reviewed the initial assessment and has concerns that require additional information.
Generate targeted questions to address these specific concerns.

OUTPUT FORMAT: Same JSON structure as the initial questionnaire generation.
Focus only on questions that address the stakeholder's feedback - do not repeat previous questions.
"""


class QuestionnaireGeneratorAgent:
    """
    Agent 1: Generates ECDD questionnaires from client profiles.
    
    Uses Azure AI Foundry for intelligent question generation based on:
    - Client profile analysis
    - Risk factor identification
    - Information gap detection
    """
    
    def __init__(self, project_endpoint: str = PROJECT_ENDPOINT):
        self.project_endpoint = project_endpoint
        self._client = None
        self._agent = None
        self._initialized = False
    
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
            
            # Try to get existing agent or create new one
            self._agent = self._get_or_create_agent()
            self._initialized = True
            logger.info("QuestionnaireGeneratorAgent initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize QuestionnaireGeneratorAgent: {e}")
            return False
    
    def _get_or_create_agent(self):
        """Get existing agent or create a new one."""
        # Try to find existing agent
        try:
            agents = self._client.agents.list_agents()
            for agent in agents.data:
                if agent.name == QUESTIONNAIRE_AGENT_NAME:
                    logger.info(f"Found existing questionnaire agent: {agent.id}")
                    return agent
        except Exception as e:
            logger.warning(f"Could not list agents: {e}")
        
        # Create new agent
        agent = self._client.agents.create_agent(
            model=MODEL_DEPLOYMENT,
            name=QUESTIONNAIRE_AGENT_NAME,
            instructions=QUESTIONNAIRE_SYSTEM_PROMPT,
            temperature=0.3  # Lower temperature for consistent JSON output
        )
        logger.info(f"Created new questionnaire agent: {agent.id}")
        return agent
    
    def generate_questionnaire(
        self, 
        client_profile: ClientProfile
    ) -> DynamicQuestionnaire:
        """
        Generate a dynamic questionnaire based on client profile.
        
        Args:
            client_profile: The client profile to analyze
            
        Returns:
            DynamicQuestionnaire with sections and questions
        """
        if not self._initialized:
            self.initialize()
        
        # Build the prompt with profile summary
        profile_summary = client_profile.get_summary_for_agent()
        known_info = self._extract_known_info(client_profile)
        
        prompt = f"""Analyze this client profile and generate an ECDD questionnaire.

CLIENT PROFILE:
{profile_summary}

ALREADY KNOWN INFORMATION (skip questions for these):
{known_info}

Generate a comprehensive questionnaire focusing on:
1. Any gaps in identity verification
2. Source of Wealth verification
3. Source of Funds for expected transactions
4. Business/employment details
5. Expected account activity
6. Any areas flagged by PEP/sanctions/adverse news screening

Return ONLY valid JSON in the specified format."""

        try:
            # Create thread and run
            thread = self._client.agents.create_thread()
            
            self._client.agents.create_message(
                thread_id=thread.id,
                role="user",
                content=prompt
            )
            
            run = self._client.agents.create_and_process_run(
                thread_id=thread.id,
                agent_id=self._agent.id
            )
            
            # Get response
            messages = self._client.agents.list_messages(thread_id=thread.id)
            response_text = ""
            for msg in messages.data:
                if msg.role == "assistant":
                    for content in msg.content:
                        if hasattr(content, 'text'):
                            response_text = content.text.value
                            break
            
            # Parse JSON response
            return self._parse_questionnaire_response(
                response_text,
                client_profile.customer_id,
                client_profile.customer_name
            )
            
        except Exception as e:
            logger.error(f"Error generating questionnaire: {e}")
            return self._generate_fallback_questionnaire(client_profile)
    
    def generate_followup_questionnaire(
        self,
        client_profile: ClientProfile,
        stakeholder_feedback: str,
        previous_responses: Dict[str, Any]
    ) -> DynamicQuestionnaire:
        """
        Generate follow-up questions based on stakeholder feedback.
        
        Args:
            client_profile: Original client profile
            stakeholder_feedback: Stakeholder's concerns/questions
            previous_responses: Responses from initial questionnaire
            
        Returns:
            DynamicQuestionnaire with follow-up questions
        """
        if not self._initialized:
            self.initialize()
        
        prompt = f"""Generate FOLLOW-UP ECDD questions based on stakeholder feedback.

CLIENT: {client_profile.customer_name} (ID: {client_profile.customer_id})

STAKEHOLDER FEEDBACK/CONCERNS:
{stakeholder_feedback}

PREVIOUS RESPONSES SUMMARY:
{json.dumps(previous_responses, indent=2)[:2000]}  # Truncate if too long

Generate targeted questions to address ONLY the stakeholder's specific concerns.
Return ONLY valid JSON in the specified format."""

        try:
            thread = self._client.agents.create_thread()
            
            # Use follow-up system prompt
            self._client.agents.create_message(
                thread_id=thread.id,
                role="user",
                content=FOLLOWUP_SYSTEM_PROMPT + "\n\n" + prompt
            )
            
            run = self._client.agents.create_and_process_run(
                thread_id=thread.id,
                agent_id=self._agent.id
            )
            
            messages = self._client.agents.list_messages(thread_id=thread.id)
            response_text = ""
            for msg in messages.data:
                if msg.role == "assistant":
                    for content in msg.content:
                        if hasattr(content, 'text'):
                            response_text = content.text.value
                            break
            
            questionnaire = self._parse_questionnaire_response(
                response_text,
                client_profile.customer_id,
                client_profile.customer_name
            )
            
            # Mark as follow-up
            questionnaire.questionnaire_id = f"followup-{questionnaire.questionnaire_id}"
            return questionnaire
            
        except Exception as e:
            logger.error(f"Error generating follow-up questionnaire: {e}")
            return self._generate_fallback_questionnaire(client_profile, is_followup=True)
    
    def _extract_known_info(self, profile: ClientProfile) -> str:
        """Extract information already known from the profile."""
        known = []
        
        if profile.identity.full_name:
            known.append(f"- Full Name: {profile.identity.full_name}")
        if profile.identity.nationality:
            known.append(f"- Nationality: {profile.identity.nationality}")
        if profile.identity.residence_country:
            known.append(f"- Residence Country: {profile.identity.residence_country}")
        if profile.identity.dob:
            known.append(f"- Date of Birth: {profile.identity.dob}")
        if profile.identity.id_type and profile.identity.id_number:
            known.append(f"- ID Verified: {profile.identity.id_type}")
        
        if profile.pep_status:
            pep = profile.pep_status[0]
            known.append(f"- PEP Status: {'Yes' if pep.is_pep else 'No'}")
        
        if profile.sanctions:
            known.append(f"- Sanctions screening completed ({len(profile.sanctions)} results)")
        
        if profile.adverse_news:
            known.append(f"- Adverse news screening completed ({len(profile.adverse_news)} items)")
        
        return "\n".join(known) if known else "Minimal information available - comprehensive questionnaire needed."
    
    def _parse_questionnaire_response(
        self, 
        response: str, 
        customer_id: str,
        customer_name: str
    ) -> DynamicQuestionnaire:
        """Parse AI response JSON into DynamicQuestionnaire."""
        try:
            # Clean up response - extract JSON from markdown if needed
            json_text = response
            if "```json" in response:
                json_text = response.split("```json")[1].split("```")[0]
            elif "```" in response:
                json_text = response.split("```")[1].split("```")[0]
            
            data = json.loads(json_text.strip())
            
            sections = []
            for s in data.get("sections", []):
                questions = []
                for q in s.get("questions", []):
                    questions.append(QuestionField(
                        field_id=q.get("field_id", str(uuid.uuid4())[:8]),
                        question_text=q.get("question_text", ""),
                        question_type=QuestionType(q.get("question_type", "text")),
                        required=q.get("required", True),
                        help_text=q.get("help_text", ""),
                        options=q.get("options", []),
                        category=q.get("category", ""),
                        aml_relevant=q.get("aml_relevant", False)
                    ))
                
                sections.append(QuestionSection(
                    section_id=s.get("section_id", str(uuid.uuid4())[:8]),
                    section_title=s.get("section_title", "Questions"),
                    section_description=s.get("section_description", ""),
                    section_icon=s.get("section_icon", "📋"),
                    order=s.get("order", 0),
                    questions=questions
                ))
            
            return DynamicQuestionnaire(
                questionnaire_id=str(uuid.uuid4()),
                customer_id=customer_id,
                customer_name=customer_name,
                sections=sections,
                client_type=data.get("client_type", ""),
                profile_summary={"parsed": True}
            )
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse questionnaire JSON: {e}")
            return self._generate_fallback_questionnaire(
                ClientProfile(customer_id=customer_id, customer_name=customer_name)
            )
    
    def _generate_fallback_questionnaire(
        self, 
        profile: ClientProfile,
        is_followup: bool = False
    ) -> DynamicQuestionnaire:
        """Generate a fallback questionnaire if AI generation fails."""
        prefix = "followup-" if is_followup else ""
        
        sections = [
            QuestionSection(
                section_id=f"{prefix}identity",
                section_title="Identity Verification",
                section_description="Confirm client identity details",
                section_icon="🪪",
                order=1,
                questions=[
                    QuestionField(
                        field_id=f"{prefix}id_verified",
                        question_text="Have you verified the client's identity documents?",
                        question_type=QuestionType.YES_NO,
                        required=True,
                        category="identity",
                        aml_relevant=True
                    ),
                ]
            ),
            QuestionSection(
                section_id=f"{prefix}sow",
                section_title="Source of Wealth",
                section_description="Verify the origin of the client's wealth",
                section_icon="💰",
                order=2,
                questions=[
                    QuestionField(
                        field_id=f"{prefix}primary_sow",
                        question_text="What is the primary source of the client's wealth?",
                        question_type=QuestionType.DROPDOWN,
                        required=True,
                        options=["Employment", "Business Ownership", "Inheritance", "Investments", "Sale of Property", "Other"],
                        category="sow",
                        aml_relevant=True
                    ),
                    QuestionField(
                        field_id=f"{prefix}sow_documentation",
                        question_text="What documentation can the client provide to verify source of wealth?",
                        question_type=QuestionType.TEXTAREA,
                        required=True,
                        category="sow",
                        aml_relevant=True
                    ),
                ]
            ),
            QuestionSection(
                section_id=f"{prefix}sof",
                section_title="Source of Funds",
                section_description="Verify the source of funds for transactions",
                section_icon="💵",
                order=3,
                questions=[
                    QuestionField(
                        field_id=f"{prefix}expected_activity",
                        question_text="What is the expected monthly transaction volume?",
                        question_type=QuestionType.DROPDOWN,
                        required=True,
                        options=["< $10,000", "$10,000 - $50,000", "$50,000 - $100,000", "$100,000 - $500,000", "> $500,000"],
                        category="sof",
                        aml_relevant=True
                    ),
                ]
            ),
        ]
        
        return DynamicQuestionnaire(
            questionnaire_id=f"{prefix}{str(uuid.uuid4())}",
            customer_id=profile.customer_id,
            customer_name=profile.customer_name,
            sections=sections,
            client_type="Unknown",
            profile_summary={"fallback": True}
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Get agent status."""
        return {
            "name": "QuestionnaireGeneratorAgent",
            "initialized": self._initialized,
            "agent_id": self._agent.id if self._agent else None,
            "endpoint": self.project_endpoint
        }
