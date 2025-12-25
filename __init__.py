"""
ECDD - Enhanced Client Due Diligence Module

A modular, clean architecture for ECDD questionnaire generation, 
analysis, and reporting. This module is standalone and does not 
depend on any orchestrator components.

Architecture:
- schemas.py: Flexible data models (easily configurable client profiles)
- questionnaire_agent.py: Agent 1 - Generates questions from client context
- reporter_agent.py: Agent 2 - Generates checklists and ECDD Assessment reports
- agents.py: Coordinator that provides single interface to both agents
- databricks.py: Unity Catalog connector for profiles and structured storage
- exporter.py: PDF/JSON exporter with Volumes writing
- session.py: Session management
- app.py: Streamlit application
"""

__version__ = "2.0.0"
__author__ = "ECDD Team"

from .schemas import (
    ClientProfile,
    IdentityProfile,
    ComplianceFlags,
    ECDDAssessment,
    DocumentChecklist,
    QuestionnaireSession,
    ECDDOutput
)

from .agents import ECDDAgentCoordinator
from .databricks import DatabricksConnector, DatabricksConfig
from .exporter import ECDDExporter
from .session import ECDDSessionManager

__all__ = [
    # Schemas
    "ClientProfile",
    "IdentityProfile", 
    "ComplianceFlags",
    "ECDDAssessment",
    "DocumentChecklist",
    "QuestionnaireSession",
    "ECDDOutput",
    # Agents
    "ECDDAgentCoordinator",
    # Infrastructure
    "DatabricksConnector",
    "DatabricksConfig",
    "ECDDExporter",
    "ECDDSessionManager",
]
