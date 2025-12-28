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
- utils.py: Type coercion utilities for robust data handling
- app.py: Streamlit application
"""

__version__ = "2.1.0"
__author__ = "ECDD Team"

from .schemas import (
    ClientProfile,
    IdentityProfile,
    ComplianceFlags,
    ECDDAssessment,
    DocumentChecklist,
    QuestionnaireSession,
    ECDDOutput,
    SessionStatus,
    RiskLevel,
    DynamicQuestionnaire,
)

from .agents import ECDDAgentCoordinator
from .databricks import DatabricksConnector, DatabricksConfig
from .session import ECDDSessionManager

# Exporter depends on reportlab - import conditionally
try:
    from .exporter import ECDDExporter
except Exception as e:
    ECDDExporter = None
    print(f"Warning: ECDDExporter not available due to: {e}")

# Type coercion utilities
from .utils import (
    ensure_string_timestamp,
    format_display_date,
    format_display_datetime,
    ensure_string,
    ensure_float,
    ensure_int,
    ensure_bool,
    ensure_list,
    ensure_dict,
    safe_json_dumps,
    safe_json_loads,
)

__all__ = [
    # Schemas
    "ClientProfile",
    "IdentityProfile", 
    "ComplianceFlags",
    "ECDDAssessment",
    "DocumentChecklist",
    "QuestionnaireSession",
    "ECDDOutput",
    "SessionStatus",
    "RiskLevel",
    "DynamicQuestionnaire",
    # Agents
    "ECDDAgentCoordinator",
    # Infrastructure
    "DatabricksConnector",
    "DatabricksConfig",
    "ECDDExporter",
    "ECDDSessionManager",
    # Utils
    "ensure_string_timestamp",
    "format_display_date",
    "format_display_datetime",
    "ensure_string",
    "ensure_float",
    "safe_json_dumps",
    "safe_json_loads",
]
