"""
ECDD Session Manager

Handles session state persistence with JSON fallback.
No orchestrator dependencies.
"""

import os
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

from .schemas import (
    QuestionnaireSession,
    SessionStatus,
    ClientProfile,
    DynamicQuestionnaire,
    ECDDAssessment,
    DocumentChecklist,
)

logger = logging.getLogger(__name__)

# Configuration
SESSION_STORAGE_PATH = os.environ.get("ECDD_SESSION_PATH", "./sessions")


class ECDDSessionManager:
    """
    Manages ECDD sessions with persistence.

    Features:
    - Checkpoint-based saves (cost-effective)
    - JSON file fallback
    - Thread-safe for Streamlit multi-process
    """

    def __init__(self, storage_path: str = SESSION_STORAGE_PATH):
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)
        self._sessions: Dict[str, QuestionnaireSession] = {}
        self._write_buffer: Dict[str, bool] = {}  # session_id -> needs_save

    def _get_session_path(self, session_id: str) -> str:
        """Get file path for a session."""
        return os.path.join(self.storage_path, f"{session_id}.json")

    # =========================================================================
    # SESSION LIFECYCLE
    # =========================================================================

    def create_session(
        self,
        client_profile: ClientProfile,
        questionnaire: DynamicQuestionnaire = None,
        session_id: Optional[str] = None
    ) -> QuestionnaireSession:
        """
        Create a new ECDD session.

        Args:
            client_profile: Client profile data
            questionnaire: Optional generated questionnaire

        Returns:
            New QuestionnaireSession
        """
        # IMPORTANT: allow caller (e.g. agent coordinator) to provide the
        # session_id so Streamlit UI, agent sessions, and persistence all
        # reference the same identifier.
        session_id = session_id or str(uuid.uuid4())

        session = QuestionnaireSession(
            session_id=session_id,
            customer_id=client_profile.customer_id,
            customer_name=client_profile.customer_name,
            status=SessionStatus.QUESTIONNAIRE_GENERATED if questionnaire else SessionStatus.PENDING,
            client_profile=client_profile.model_dump(),
            questionnaire=questionnaire
        )

        self._sessions[session_id] = session
        self._save_session(session, checkpoint="created")

        logger.info(
            f"Created session {session_id} for {client_profile.customer_name}")
        return session

    def get_session(self, session_id: str) -> Optional[QuestionnaireSession]:
        """
        Get session by ID.

        Args:
            session_id: Session ID

        Returns:
            QuestionnaireSession or None
        """
        # Check memory cache first
        if session_id in self._sessions:
            return self._sessions[session_id]

        # Try to load from disk
        session = self._load_session(session_id)
        if session:
            self._sessions[session_id] = session

        return session

    def update_session(
        self,
        session_id: str,
        status: SessionStatus = None,
        responses: Dict[str, Any] = None,
        ecdd_assessment: ECDDAssessment = None,
        document_checklist: DocumentChecklist = None,
        review_decision: str = None,
        review_notes: str = None,
        reviewed_by: str = None,
        save_immediately: bool = False
    ) -> Optional[QuestionnaireSession]:
        """
        Update a session with new data.

        Args:
            session_id: Session ID
            status: New status
            responses: Questionnaire responses
            ecdd_assessment: Generated assessment
            document_checklist: Generated checklist
            review_decision: Review decision
            review_notes: Review notes
            reviewed_by: Reviewer ID/name
            save_immediately: Force immediate save

        Returns:
            Updated session or None
        """
        session = self.get_session(session_id)
        if not session:
            logger.warning(f"Session {session_id} not found")
            return None

        # Update fields
        if status:
            session.status = status
        if responses:
            session.responses = responses
        if ecdd_assessment:
            session.ecdd_assessment = ecdd_assessment
        if document_checklist:
            session.document_checklist = document_checklist
        if review_decision:
            session.review_decision = review_decision
            session.reviewed_at = datetime.now(timezone.utc).isoformat()
        if review_notes:
            session.review_notes = review_notes
        if reviewed_by:
            session.reviewed_by = reviewed_by

        session.updated_at = datetime.now(timezone.utc).isoformat()

        # Mark for save or save immediately
        if save_immediately:
            self._save_session(session, checkpoint="updated")
        else:
            self._write_buffer[session_id] = True

        return session

    def save_checkpoint(
        self,
        session_id: str,
        checkpoint: str = "checkpoint"
    ) -> bool:
        """
        Save session at a checkpoint milestone.

        Args:
            session_id: Session ID
            checkpoint: Checkpoint name for logging

        Returns:
            True if saved successfully
        """
        session = self.get_session(session_id)
        if not session:
            return False

        self._save_session(session, checkpoint=checkpoint)

        # Clear from write buffer
        if session_id in self._write_buffer:
            del self._write_buffer[session_id]

        return True

    def flush_all(self):
        """Flush all pending session saves."""
        for session_id in list(self._write_buffer.keys()):
            if self._write_buffer.get(session_id):
                self.save_checkpoint(session_id, "flush")
        self._write_buffer.clear()

    # =========================================================================
    # PERSISTENCE
    # =========================================================================

    def _save_session(self, session: QuestionnaireSession, checkpoint: str = None):
        """Save session to JSON file."""
        filepath = self._get_session_path(session.session_id)

        try:
            data = session.model_dump()
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)

            logger.debug(f"Saved session {session.session_id} [{checkpoint}]")
        except Exception as e:
            logger.error(f"Failed to save session {session.session_id}: {e}")

    def _load_session(self, session_id: str) -> Optional[QuestionnaireSession]:
        """Load session from JSON file."""
        filepath = self._get_session_path(session_id)

        if not os.path.exists(filepath):
            return None

        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

            # Reconstruct nested objects
            session = QuestionnaireSession(**data)
            logger.debug(f"Loaded session {session_id}")
            return session

        except Exception as e:
            logger.error(f"Failed to load session {session_id}: {e}")
            return None

    def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        filepath = self._get_session_path(session_id)

        # Remove from memory
        if session_id in self._sessions:
            del self._sessions[session_id]
        if session_id in self._write_buffer:
            del self._write_buffer[session_id]

        # Remove file
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                logger.info(f"Deleted session {session_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to delete session {session_id}: {e}")

        return False

    # =========================================================================
    # LISTING AND SEARCH
    # =========================================================================

    def list_sessions(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        List recent sessions.

        Args:
            limit: Maximum number of sessions to return

        Returns:
            List of session summaries
        """
        sessions = []

        try:
            files = sorted(
                [f for f in os.listdir(self.storage_path)
                 if f.endswith('.json')],
                key=lambda f: os.path.getmtime(
                    os.path.join(self.storage_path, f)),
                reverse=True
            )

            for filename in files[:limit]:
                session_id = filename.replace('.json', '')
                session = self.get_session(session_id)
                if session:
                    sessions.append({
                        'session_id': session.session_id,
                        'customer_id': session.customer_id,
                        'customer_name': session.customer_name,
                        'status': session.status.value,
                        'created_at': session.created_at,
                        'updated_at': session.updated_at,
                    })
        except Exception as e:
            logger.error(f"Error listing sessions: {e}")

        return sessions

    def list_pending_reviews(self) -> List[QuestionnaireSession]:
        """List sessions pending stakeholder review."""
        pending = []

        for summary in self.list_sessions(limit=100):
            if summary['status'] == SessionStatus.REPORTS_GENERATED.value:
                session = self.get_session(summary['session_id'])
                if session:
                    pending.append(session)

        return pending

    def search_sessions(
        self,
        customer_id: str = None,
        customer_name: str = None,
        status: SessionStatus = None
    ) -> List[QuestionnaireSession]:
        """
        Search sessions by criteria.

        Args:
            customer_id: Filter by customer ID
            customer_name: Filter by customer name (partial match)
            status: Filter by status

        Returns:
            Matching sessions
        """
        results = []

        for summary in self.list_sessions(limit=100):
            session = self.get_session(summary['session_id'])
            if not session:
                continue

            if customer_id and session.customer_id != customer_id:
                continue
            if customer_name and customer_name.lower() not in session.customer_name.lower():
                continue
            if status and session.status != status:
                continue

            results.append(session)

        return results

    def get_customer_history(
        self,
        customer_id: str,
        include_followups: bool = True
    ) -> List[QuestionnaireSession]:
        """
        Get all ECDD sessions for a customer (for periodic reviews).

        Args:
            customer_id: Customer ID to look up
            include_followups: Whether to include follow-up sessions

        Returns:
            List of sessions sorted by date (newest first)
        """
        sessions = self.search_sessions(customer_id=customer_id)

        if not include_followups:
            sessions = [s for s in sessions if not s.is_followup]

        # Sort by created_at descending
        sessions.sort(key=lambda s: s.created_at, reverse=True)

        return sessions

    def get_latest_assessment_for_customer(
        self,
        customer_id: str
    ) -> Optional[ECDDAssessment]:
        """
        Get the most recent completed assessment for a customer.

        Args:
            customer_id: Customer ID

        Returns:
            Latest ECDDAssessment or None
        """
        history = self.get_customer_history(
            customer_id, include_followups=True)

        for session in history:
            if session.ecdd_assessment and session.status in [
                SessionStatus.REPORTS_GENERATED,
                SessionStatus.APPROVED,
                SessionStatus.ESCALATED
            ]:
                return session.ecdd_assessment

        return None

    def get_session_chain(self, session_id: str) -> List[QuestionnaireSession]:
        """
        Get the full chain of sessions (parent + all follow-ups).

        Useful for seeing the complete assessment history for one review cycle.

        Args:
            session_id: Any session in the chain

        Returns:
            List of sessions in the chain, ordered by creation
        """
        session = self.get_session(session_id)
        if not session:
            return []

        # Find root session
        root_id = session_id
        while session and session.parent_session_id:
            parent = self.get_session(session.parent_session_id)
            if parent:
                root_id = parent.session_id
                session = parent
            else:
                break

        # Get all sessions for this customer and filter to chain
        customer_sessions = self.search_sessions(
            customer_id=session.customer_id)

        chain = []

        def add_to_chain(sid: str):
            for s in customer_sessions:
                if s.session_id == sid:
                    chain.append(s)
                    # Find children
                    for child in customer_sessions:
                        if child.parent_session_id == sid:
                            add_to_chain(child.session_id)
                    break

        add_to_chain(root_id)

        # Sort by created_at
        chain.sort(key=lambda s: s.created_at)
        return chain


# =============================================================================
# SINGLETON ACCESSOR
# =============================================================================

_session_manager_instance: Optional[ECDDSessionManager] = None


def get_session_manager(storage_path: str = None) -> ECDDSessionManager:
    """Get singleton session manager instance."""
    global _session_manager_instance
    if _session_manager_instance is None:
        _session_manager_instance = ECDDSessionManager(
            storage_path or SESSION_STORAGE_PATH)
    return _session_manager_instance
