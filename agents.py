
# agents.py
"""
ECDD Agent Coordinator
Provides a single interface to work with both agents:
- Agent 1: Questionnaire Generator
- Agent 2: Reporter & Validator
This ensures both agents work hand-in-hand while maintaining
clear separation of concerns.
"""
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import uuid

from .schemas import (
    ClientProfile,
    DynamicQuestionnaire,
    ECDDAssessment,
    DocumentChecklist,
    QuestionnaireSession,
    ECDDOutput,
    SessionStatus,
)
from .questionnaire_agent import QuestionnaireGeneratorAgent
    # (async agent)
from .reporter_agent import ReporterValidatorAgent
    # (async agent)

class ECDDAgentCoordinator:
    """
    Unified interface for ECDD agent operations.
    Coordinates between:
    - QuestionnaireGeneratorAgent (Agent 1): Generates questions
    - ReporterValidatorAgent (Agent 2): Generates reports

    Usage:
      coordinator = ECDDAgentCoordinator()
      await coordinator.initialize()

      # Step 1: Generate questionnaire
      session = await coordinator.create_session(client_profile)

      # Step 2: After responses collected, generate assessment
      assessment, checklist = await coordinator.submit_responses(session_id, responses)

      # Optional: Handle stakeholder queries
      answer = await coordinator.answer_query(session_id, "What is the PEP status?")

      # Optional: Generate follow-up questions
      followup = await coordinator.generate_followup(session_id, "Need more SOW details")
    """

    def __init__(self, project_endpoint: str = None):
        self.project_endpoint = project_endpoint
        self._questionnaire_agent = (
            QuestionnaireGeneratorAgent(project_endpoint)
            if project_endpoint else QuestionnaireGeneratorAgent()
        )
        self._reporter_agent = (
            ReporterValidatorAgent(project_endpoint)
            if project_endpoint else ReporterValidatorAgent()
        )
        self._sessions: Dict[str, QuestionnaireSession] = {}
        self._initialized = False

    async def initialize(self) -> bool:
        """Initialize both agents (async)."""
        try:
            q_init = await self._questionnaire_agent.initialize()
            r_init = await self._reporter_agent.initialize()
            self._initialized = bool(q_init and r_init)
            if self._initialized:
                print("ECDDAgentCoordinator initialized successfully")
            else:
                print("ECDDAgentCoordinator partially initialized")
            return self._initialized
        except Exception as e:
            print(f"Failed to initialize ECDDAgentCoordinator: {e}")
            return False

    async def create_session(
        self,
        client_profile: ClientProfile
    ) -> QuestionnaireSession:
        """
        Create a new ECDD session and generate questionnaire.
        Args:
          client_profile: The client profile to assess
        Returns:
          QuestionnaireSession with generated questionnaire
        """
        if not self._initialized:
            await self.initialize()

        session_id = str(uuid.uuid4())

        # Agent 1: Generate questionnaire (async)
        questionnaire: DynamicQuestionnaire = await self._questionnaire_agent.generate_questionnaire(client_profile)

        session = QuestionnaireSession(
            session_id=session_id,
            customer_id=client_profile.customer_id,
            customer_name=client_profile.customer_name,
            status=SessionStatus.QUESTIONNAIRE_GENERATED,
            client_profile=client_profile.model_dump(),
            questionnaire=questionnaire
        )
        self._sessions[session_id] = session
        print(f"Created session {session_id} with {questionnaire.get_total_questions()} questions")
        return session

    async def submit_responses(
        self,
        session_id: str,
        responses: Dict[str, Any]
    ) -> Tuple[ECDDAssessment, DocumentChecklist]:
        """
        Submit questionnaire responses and generate assessment.
        Args:
          session_id: The session ID
          responses: Dict of field_id -> response value
        Returns:
          Tuple of (ECDDAssessment, DocumentChecklist)
        """
        if session_id not in self._sessions:
            raise ValueError(f"Session {session_id} not found")

        session = self._sessions[session_id]
        session.responses = responses
        session.status = SessionStatus.RESPONSES_SUBMITTED
        session.updated_at = datetime.now(timezone.utc).isoformat()

        # Reconstruct client profile from session
        client_profile = ClientProfile(**session.client_profile)

        # Agent 2: Generate assessment and checklist (async)
        assessment, checklist = await self._reporter_agent.generate_assessment(
            client_profile=client_profile,
            questionnaire_responses=responses,
            session_id=session_id
        )

        # Update session
        session.ecdd_assessment = assessment
        session.document_checklist = checklist
        session.status = SessionStatus.REPORTS_GENERATED
        session.updated_at = datetime.now(timezone.utc).isoformat()

        print(f"Generated assessment for session {session_id}: {assessment.overall_risk_rating.value}")
        return assessment, checklist

    async def get_ecdd_output(self, session_id: str) -> ECDDOutput:
        """
        Get complete ECDD output for persistence.
        Args:
          session_id: The session ID
        Returns:
          ECDDOutput ready for Delta Table storage
        """
        if session_id not in self._sessions:
            raise ValueError(f"Session {session_id} not found")

        session = self._sessions[session_id]
        if not session.ecdd_assessment or not session.document_checklist:
            raise ValueError("Assessment not yet generated. Call submit_responses first.")

        return ECDDOutput(
            session_id=session_id,
            customer_id=session.customer_id,
            customer_name=session.customer_name,
            compliance_flags=session.ecdd_assessment.compliance_flags,
            ecdd_assessment=session.ecdd_assessment,
            document_checklist=session.document_checklist,
            questionnaire_responses=session.responses,
            status=session.status.value,
            review_decision=session.review_decision,
            reviewed_by=session.reviewed_by,
            reviewed_at=session.reviewed_at
        )

    async def answer_query(
        self,
        session_id: str,
        question: str
    ) -> str:
        """
        Answer a stakeholder query about the assessment.
        Args:
          session_id: The session ID
          question: The stakeholder's question
        Returns:
          Answer as string
        """
        if session_id not in self._sessions:
            raise ValueError(f"Session {session_id} not found")

        session = self._sessions[session_id]
        if not session.ecdd_assessment:
            return "Assessment has not been generated yet. Please complete the questionnaire first."

        client_profile = ClientProfile(**session.client_profile)

        # Agent 2: answer query (async)
        return await self._reporter_agent.answer_stakeholder_query(
            session_id=session_id,
            question=question,
            client_profile=client_profile,
            assessment=session.ecdd_assessment
        )

    async def generate_followup(
        self,
        session_id: str,
        stakeholder_feedback: str
    ) -> DynamicQuestionnaire:
        """
        Generate follow-up questionnaire based on stakeholder feedback.
        Args:
          session_id: The original session ID
          stakeholder_feedback: Stakeholder's concerns/requirements
        Returns:
          DynamicQuestionnaire with follow-up questions
        """
        if session_id not in self._sessions:
            raise ValueError(f"Session {session_id} not found")

        session = self._sessions[session_id]
        client_profile = ClientProfile(**session.client_profile)

        # Agent 1: Generate follow-up questionnaire (async)
        followup = await self._questionnaire_agent.generate_followup_questionnaire(
            client_profile=client_profile,
            stakeholder_feedback=stakeholder_feedback,
            previous_responses=session.responses
        )

        # Create new session for follow-up
        followup_session_id = str(uuid.uuid4())
        followup_session = QuestionnaireSession(
            session_id=followup_session_id,
            customer_id=session.customer_id,
            customer_name=session.customer_name,
            status=SessionStatus.QUESTIONNAIRE_GENERATED,
            client_profile=session.client_profile,
            questionnaire=followup,
            is_followup=True,
            parent_session_id=session_id
        )
        self._sessions[followup_session_id] = followup_session
        print(f"Created follow-up session {followup_session_id}")
        return followup

    async def complete_review(
        self,
        session_id: str,
        decision: str,
        notes: str = None,
        reviewer: str = None,
        reviewed_by: str = None,  # alias for compatibility with app
    ) -> QuestionnaireSession:
        """
        Complete stakeholder review of the assessment.
        Args:
          session_id: The session ID
          decision: "approved", "rejected", or "escalated"
          notes: Optional review notes
          reviewer: Optional reviewer name/ID
          reviewed_by: Alias for reviewer (kept for app compatibility)
        Returns:
          Updated session
        """
        if session_id not in self._sessions:
            raise ValueError(f"Session {session_id} not found")

        session = self._sessions[session_id]
        session.review_decision = decision
        session.review_notes = notes
        session.reviewed_by = reviewer or reviewed_by
        session.reviewed_at = datetime.now(timezone.utc).isoformat()

        if decision == "approved":
            session.status = SessionStatus.APPROVED
        elif decision == "rejected":
            session.status = SessionStatus.REJECTED
        elif decision == "escalated":
            session.status = SessionStatus.ESCALATED

        session.updated_at = datetime.now(timezone.utc).isoformat()
        print(f"Session {session_id} review completed: {decision}")
        return session

    async def generate_followup_and_continue(
        self,
        session_id: str,
        stakeholder_feedback: str
    ) -> Tuple[str, DynamicQuestionnaire]:
        """
        Generate follow-up questionnaire and create a linked session.
        This allows the follow-up to be processed through the same pipeline:
          1. Generate follow-up questions
          2. Collect responses (via submit_followup_responses)
          3. Merge with original assessment
        Args:
          session_id: The original session ID
          stakeholder_feedback: Stakeholder's concerns
        Returns:
          Tuple of (followup_session_id, DynamicQuestionnaire)
        """
        if session_id not in self._sessions:
            raise ValueError(f"Session {session_id} not found")

        session = self._sessions[session_id]
        client_profile = ClientProfile(**session.client_profile)

        # Agent 1: Generate follow-up questionnaire (async)
        followup = await self._questionnaire_agent.generate_followup_questionnaire(
            client_profile=client_profile,
            stakeholder_feedback=stakeholder_feedback,
            previous_responses=session.responses
        )

        # Create linked follow-up session
        followup_session_id = str(uuid.uuid4())
        followup_session = QuestionnaireSession(
            session_id=followup_session_id,
            customer_id=session.customer_id,
            customer_name=session.customer_name,
            status=SessionStatus.QUESTIONNAIRE_GENERATED,
            client_profile=session.client_profile,
            questionnaire=followup,
            is_followup=True,
            parent_session_id=session_id
        )
        self._sessions[followup_session_id] = followup_session
        print(f"Created follow-up session {followup_session_id} linked to {session_id}")
        return followup_session_id, followup

    async def submit_followup_responses(
        self,
        followup_session_id: str,
        responses: Dict[str, Any]
    ) -> Tuple[ECDDAssessment, DocumentChecklist]:
        """
        Submit follow-up responses and regenerate assessment with combined data.
        Merges follow-up responses with original session data to create
        an updated, comprehensive assessment.
        Args:
          followup_session_id: The follow-up session ID
          responses: Responses to follow-up questions
        Returns:
          Updated (ECDDAssessment, DocumentChecklist)
        """
        if followup_session_id not in self._sessions:
            raise ValueError(f"Session {followup_session_id} not found")

        followup_session = self._sessions[followup_session_id]
        if not followup_session.is_followup or not followup_session.parent_session_id:
            raise ValueError("This is not a follow-up session")

        parent_session = self._sessions.get(followup_session.parent_session_id)
        if not parent_session:
            raise ValueError(f"Parent session {followup_session.parent_session_id} not found")

        # Merge responses: original + follow-up (follow-up takes precedence)
        combined_responses = {**(parent_session.responses or {}), **responses}
        followup_session.responses = combined_responses

        client_profile = ClientProfile(**followup_session.client_profile)

        # Agent 2: Generate updated assessment with combined data (async)
        assessment, checklist = await self._reporter_agent.generate_assessment(
            client_profile=client_profile,
            questionnaire_responses=combined_responses,
            session_id=followup_session_id
        )

        # Update follow-up session
        followup_session.ecdd_assessment = assessment
        followup_session.document_checklist = checklist
        followup_session.status = SessionStatus.REPORTS_GENERATED
        followup_session.updated_at = datetime.now(timezone.utc).isoformat()

        print(f"Updated assessment for follow-up session {followup_session_id}")
        return assessment, checklist

    async def get_customer_history(
        self,
        customer_id: str,
        include_followups: bool = True
    ) -> list[QuestionnaireSession]:
        """
        Get all ECDD sessions for a customer (for periodic reviews).
        Args:
          customer_id: Customer ID to look up
          include_followups: Whether to include follow-up sessions
        Returns:
          List of sessions sorted by date (newest first)
        """
        sessions = []
        for session in self._sessions.values():
            if session.customer_id == customer_id:
                if not include_followups and session.is_followup:
                    continue
                sessions.append(session)

        # Sort by created_at descending
        sessions.sort(key=lambda s: s.created_at, reverse=True)
        return sessions

    async def get_latest_assessment_for_customer(
        self,
        customer_id: str
    ) -> Optional[ECDDAssessment]:
        """
        Get the most recent completed assessment for a customer.
        Useful for periodic reviews to compare with historical assessments.
        Args:
          customer_id: Customer ID
        Returns:
          Latest ECDDAssessment or None
        """
        history = await self.get_customer_history(customer_id, include_followups=True)
        for session in history:
            if session.ecdd_assessment and session.status in [
                SessionStatus.REPORTS_GENERATED,
                SessionStatus.APPROVED,
                SessionStatus.ESCALATED
            ]:
                return session.ecdd_assessment
        return None

    async def compare_assessments(
        self,
        current_session_id: str,
        previous_session_id: str
    ) -> Dict[str, Any]:
        """
        Compare two assessments for the same customer.
        Useful for periodic reviews to identify changes in risk profile.
        Args:
          current_session_id: Current assessment session
          previous_session_id: Previous assessment session
        Returns:
          Dict with comparison details
        """
        current = self._sessions.get(current_session_id)
        previous = self._sessions.get(previous_session_id)
        if not current or not previous:
            raise ValueError("One or both sessions not found")
        if not current.ecdd_assessment or not previous.ecdd_assessment:
            raise ValueError("One or both sessions missing assessment")

        current_assessment = current.ecdd_assessment
        previous_assessment = previous.ecdd_assessment

        comparison = {
            "customer_id": current.customer_id,
            "current_session": current_session_id,
            "previous_session": previous_session_id,
            "current_date": current.created_at,
            "previous_date": previous.created_at,
            "risk_rating_change": {
                "previous": previous_assessment.overall_risk_rating.value,
                "current": current_assessment.overall_risk_rating.value,
                "changed": previous_assessment.overall_risk_rating != current_assessment.overall_risk_rating
            },
            "risk_score_change": {
                "previous": previous_assessment.risk_score,
                "current": current_assessment.risk_score,
                "delta": current_assessment.risk_score - previous_assessment.risk_score
            },
            "compliance_flags_changes": {
                "previous": previous_assessment.compliance_flags.model_dump(),
                "current": current_assessment.compliance_flags.model_dump(),
            },
            "new_concerns": [],
            "resolved_concerns": []
        }

        # Identify flag changes
        prev_flags = previous_assessment.compliance_flags.model_dump()
        curr_flags = current_assessment.compliance_flags.model_dump()
        for flag, value in curr_flags.items():
            if value and not prev_flags.get(flag):
                comparison["new_concerns"].append(flag)
            elif not value and prev_flags.get(flag):
                comparison["resolved_concerns"].append(flag)

        return comparison

    # ----------------- utilities (sync) -----------------
    def get_session(self, session_id: str) -> Optional[QuestionnaireSession]:
        """Get session by ID."""
        return self._sessions.get(session_id)

    def list_sessions(self) -> list[QuestionnaireSession]:
        """List all sessions."""
        return list(self._sessions.values())

    def get_status(self) -> Dict[str, Any]:
        """Get coordinator and agent status."""
        return {
            "coordinator": {
                "initialized": self._initialized,
                "active_sessions": len(self._sessions)
            },
            "questionnaire_agent": self._questionnaire_agent.get_status(),
            "reporter_agent": self._reporter_agent.get_status()
        }

# =============================================================================
# SINGLETON ACCESSOR
# =============================================================================
_coordinator_instance: Optional[ECDDAgentCoordinator] = None

def get_coordinator(project_endpoint: str = None) -> ECDDAgentCoordinator:
    """Get singleton coordinator instance."""
    global _coordinator_instance
    if _coordinator_instance is None:
        _coordinator_instance = ECDDAgentCoordinator(project_endpoint)
    return _coordinator_instance
