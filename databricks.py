"""
Databricks Unity Catalog Connector

Handles:
1. Reading client profiles from Delta tables
2. Writing structured ECDD outputs to Delta tables (nested JSON)
3. Writing PDF reports to Unity Catalog Volumes

Uses Databricks CLI authentication (databricks auth login).
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

from .schemas import (
    ClientProfile,
    ProfileConfig,
    DEFAULT_PROFILE_CONFIG,
    ECDDOutput,
)

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class DatabricksConfig:
    """
    Configuration for Databricks connection and schema mapping.
    Modify this class when your delta table schema changes.
    """
    # Connection settings
    host: str = field(default_factory=lambda: os.environ.get("DATABRICKS_HOST", ""))
    http_path: str = field(default_factory=lambda: os.environ.get("DATABRICKS_HTTP_PATH", ""))
    
    # Input table configuration
    input_catalog: str = "dbx_main"
    input_schema: str = "ca_bronze"
    input_table: str = "preliminarysearch_results"
    
    # Output table configuration
    output_catalog: str = "dbx_main"
    output_schema: str = "ca_bronze"
    output_table: str = "ecdd_reports"
    
    # Volumes configuration for PDF storage
    volumes_path: str = "/Volumes/dbx_main/ca_bronze/ecdd_pdfs"
    
    # Local fallback path when Databricks unavailable
    local_fallback_path: str = "./exports"
    
    # Column mapping for INPUT
    input_columns: Dict[str, str] = field(default_factory=lambda: {
        "customer_id": "case_id",
        "customer_name": "customer_name",
        "document_type": "document_type",
        "risk_score": "risk_score",
        "profile_data": "aml_json",
        "created_at": "created_at"
    })
    
    # Column mapping for OUTPUT (structured JSON columns)
    output_columns: Dict[str, str] = field(default_factory=lambda: {
        "session_id": "session_id",
        "customer_id": "case_id",
        "customer_name": "customer_name",
        "compliance_flags": "compliance_flags_json",  # Nested JSON
        "ecdd_assessment": "ecdd_assessment_json",    # Nested JSON (not "risk_assessment")
        "document_checklist": "document_checklist_json",  # Nested JSON
        "questionnaire_responses": "questionnaire_responses_json",
        "status": "status",
        "created_at": "created_at",
        "updated_at": "updated_at",
        "review_decision": "review_decision",
        "reviewed_by": "reviewed_by",
        "reviewed_at": "reviewed_at"
    })
    
    @property
    def input_full_table(self) -> str:
        return f"{self.input_catalog}.{self.input_schema}.{self.input_table}"
    
    @property
    def output_full_table(self) -> str:
        return f"{self.output_catalog}.{self.output_schema}.{self.output_table}"


# Global default config
DEFAULT_DATABRICKS_CONFIG = DatabricksConfig()


# =============================================================================
# DATABRICKS CONNECTOR
# =============================================================================

class DatabricksConnector:
    """
    Unity Catalog connector for ECDD data operations.
    
    Features:
    - Read client profiles from Delta tables
    - Write structured ECDD outputs with nested JSON
    - Write PDF reports to Volumes
    - Fallback to local storage when Databricks unavailable
    """
    
    def __init__(
        self, 
        config: DatabricksConfig = None,
        profile_config: ProfileConfig = None
    ):
        self.config = config or DEFAULT_DATABRICKS_CONFIG
        self.profile_config = profile_config or DEFAULT_PROFILE_CONFIG
        self._connection = None
        self._workspace_client = None
        self._is_connected = False
    
    def _get_connection(self):
        """Get or create database connection."""
        if self._connection is None:
            try:
                from databricks import sql
                
                self._connection = sql.connect(
                    server_hostname=self.config.host,
                    http_path=self.config.http_path,
                )
                self._is_connected = True
                logger.info("Connected to Databricks SQL Warehouse")
            except ImportError:
                logger.warning("databricks-sql-connector not installed")
                raise
            except Exception as e:
                logger.error(f"Failed to connect to Databricks: {e}")
                raise
        
        return self._connection
    
    def _get_workspace_client(self):
        """Get or create Databricks SDK workspace client for file operations."""
        if self._workspace_client is None:
            try:
                from databricks.sdk import WorkspaceClient
                self._workspace_client = WorkspaceClient()
                logger.info("Initialized Databricks Workspace Client")
            except ImportError:
                logger.warning("databricks-sdk not installed for volume operations")
                raise
            except Exception as e:
                logger.error(f"Failed to initialize Workspace Client: {e}")
                raise
        
        return self._workspace_client
    
    def _execute_query(self, query: str) -> List[Dict]:
        """Execute a query and return results as list of dicts."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()
    
    # =========================================================================
    # READ OPERATIONS
    # =========================================================================
    
    def read_client_profile(self, customer_id: str) -> Optional[ClientProfile]:
        """
        Read a single client profile from the delta table.
        
        Args:
            customer_id: The case_id/customer_id to look up
            
        Returns:
            ClientProfile or None if not found
        """
        id_col = self.config.input_columns.get("customer_id", "case_id")
        query = f"""
            SELECT * FROM {self.config.input_full_table}
            WHERE {id_col} = '{customer_id}'
            LIMIT 1
        """
        
        try:
            results = self._execute_query(query)
            if results:
                return ClientProfile.from_databricks_row(results[0], self.profile_config)
            return None
        except Exception as e:
            logger.error(f"Error reading profile {customer_id}: {e}")
            raise
    
    def list_client_profiles(self, limit: int = 50) -> List[ClientProfile]:
        """
        List available client profiles.
        
        Args:
            limit: Maximum number of profiles to return
            
        Returns:
            List of ClientProfile objects
        """
        created_col = self.config.input_columns.get("created_at", "created_at")
        query = f"""
            SELECT * FROM {self.config.input_full_table}
            ORDER BY {created_col} DESC
            LIMIT {limit}
        """
        
        try:
            results = self._execute_query(query)
            return [ClientProfile.from_databricks_row(row, self.profile_config) for row in results]
        except Exception as e:
            logger.error(f"Error listing profiles: {e}")
            raise
    
    def search_profiles(self, search_term: str, limit: int = 20) -> List[ClientProfile]:
        """
        Search for profiles by name or ID.
        
        Args:
            search_term: Term to search for
            limit: Maximum results
            
        Returns:
            Matching profiles
        """
        id_col = self.config.input_columns.get("customer_id", "case_id")
        name_col = self.config.input_columns.get("customer_name", "customer_name")
        
        query = f"""
            SELECT * FROM {self.config.input_full_table}
            WHERE LOWER({name_col}) LIKE LOWER('%{search_term}%')
               OR LOWER({id_col}) LIKE LOWER('%{search_term}%')
            LIMIT {limit}
        """
        
        try:
            results = self._execute_query(query)
            return [ClientProfile.from_databricks_row(row, self.profile_config) for row in results]
        except Exception as e:
            logger.error(f"Error searching profiles: {e}")
            raise
    
    # =========================================================================
    # WRITE OPERATIONS - STRUCTURED JSON
    # =========================================================================
    
    def write_ecdd_output(self, output: ECDDOutput) -> str:
        """
        Write ECDD output to the reports delta table with structured JSON.
        
        The output is stored with nested JSON columns for:
        - compliance_flags
        - ecdd_assessment
        - document_checklist
        
        Args:
            output: The complete ECDD output to persist
            
        Returns:
            The session_id of the written record
        """
        cols = self.config.output_columns
        
        # Build INSERT with structured JSON values
        columns = []
        values = []
        
        # Session identifiers
        columns.append(cols.get("session_id", "session_id"))
        values.append(f"'{output.session_id}'")
        
        columns.append(cols.get("customer_id", "case_id"))
        values.append(f"'{output.customer_id}'")
        
        columns.append(cols.get("customer_name", "customer_name"))
        values.append(f"'{output.customer_name}'")
        
        # Structured JSON columns - stored as nested JSON
        columns.append(cols.get("compliance_flags", "compliance_flags_json"))
        flags_json = output.compliance_flags.model_dump_json().replace("'", "''")
        values.append(f"'{flags_json}'")
        
        columns.append(cols.get("ecdd_assessment", "ecdd_assessment_json"))
        assessment_json = output.ecdd_assessment.model_dump_json().replace("'", "''")
        values.append(f"'{assessment_json}'")
        
        columns.append(cols.get("document_checklist", "document_checklist_json"))
        checklist_json = output.document_checklist.model_dump_json().replace("'", "''")
        values.append(f"'{checklist_json}'")
        
        columns.append(cols.get("questionnaire_responses", "questionnaire_responses_json"))
        qa_json = json.dumps(output.questionnaire_responses).replace("'", "''")
        values.append(f"'{qa_json}'")
        
        # Status and timestamps
        columns.append(cols.get("status", "status"))
        values.append(f"'{output.status}'")
        
        columns.append(cols.get("created_at", "created_at"))
        values.append(f"'{output.created_at}'")
        
        columns.append(cols.get("updated_at", "updated_at"))
        values.append(f"'{output.updated_at}'")
        
        # Review info (optional)
        if output.review_decision:
            columns.append(cols.get("review_decision", "review_decision"))
            values.append(f"'{output.review_decision}'")
        
        if output.reviewed_by:
            columns.append(cols.get("reviewed_by", "reviewed_by"))
            values.append(f"'{output.reviewed_by}'")
        
        if output.reviewed_at:
            columns.append(cols.get("reviewed_at", "reviewed_at"))
            values.append(f"'{output.reviewed_at}'")
        
        query = f"""
            INSERT INTO {self.config.output_full_table}
            ({', '.join(columns)})
            VALUES ({', '.join(values)})
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.close()
            logger.info(f"Wrote ECDD output for session {output.session_id}")
            return output.session_id
        except Exception as e:
            logger.error(f"Error writing ECDD output: {e}")
            # Fallback to local JSON
            return self._write_ecdd_output_local(output)
    
    def _write_ecdd_output_local(self, output: ECDDOutput) -> str:
        """Fallback: Write ECDD output to local JSON file."""
        os.makedirs(self.config.local_fallback_path, exist_ok=True)
        filepath = os.path.join(
            self.config.local_fallback_path, 
            f"{output.session_id}_output.json"
        )
        
        with open(filepath, 'w') as f:
            json.dump(output.model_dump(), f, indent=2, default=str)
        
        logger.info(f"Wrote ECDD output locally (Databricks unavailable): {filepath}")
        return output.session_id
    
    # =========================================================================
    # PDF VOLUMES OPERATIONS
    # =========================================================================
    
    def write_pdf_to_volume(
        self, 
        session_id: str, 
        pdf_bytes: bytes, 
        filename: str
    ) -> str:
        """
        Write a PDF file to Unity Catalog Volume.
        
        Args:
            session_id: Session ID for organization
            pdf_bytes: The PDF content as bytes
            filename: Filename to use (e.g., "ecdd_assessment.pdf")
            
        Returns:
            The full path where the file was saved
        """
        try:
            w = self._get_workspace_client()
            
            # Build the volume path with session subdirectory
            full_path = f"{self.config.volumes_path}/{session_id}/{filename}"
            
            # Upload file
            w.files.upload(full_path, pdf_bytes, overwrite=True)
            
            logger.info(f"Wrote PDF to volume: {full_path}")
            return full_path
            
        except ImportError:
            logger.warning("databricks-sdk not installed, falling back to local storage")
            return self._write_pdf_local(session_id, pdf_bytes, filename)
        except Exception as e:
            logger.warning(f"Volume write failed, falling back to local: {e}")
            return self._write_pdf_local(session_id, pdf_bytes, filename)
    
    def _write_pdf_local(
        self, 
        session_id: str, 
        pdf_bytes: bytes, 
        filename: str
    ) -> str:
        """Fallback: Write PDF to local file system."""
        local_dir = os.path.join(self.config.local_fallback_path, session_id)
        os.makedirs(local_dir, exist_ok=True)
        
        filepath = os.path.join(local_dir, filename)
        with open(filepath, 'wb') as f:
            f.write(pdf_bytes)
        
        logger.info(f"Saved PDF locally (Volumes unavailable): {filepath}")
        return filepath
    
    def list_session_files(self, session_id: str) -> List[str]:
        """
        List files in a session's volume directory.
        
        Args:
            session_id: Session ID
            
        Returns:
            List of file paths
        """
        try:
            w = self._get_workspace_client()
            path = f"{self.config.volumes_path}/{session_id}"
            
            files = []
            for entry in w.files.list_directory_contents(path):
                files.append(entry.path)
            return files
            
        except Exception as e:
            logger.warning(f"Could not list volume files: {e}")
            # Try local
            local_dir = os.path.join(self.config.local_fallback_path, session_id)
            if os.path.exists(local_dir):
                return [os.path.join(local_dir, f) for f in os.listdir(local_dir)]
            return []
    
    # =========================================================================
    # CUSTOMER ECDD HISTORY (FOR PERIODIC REVIEWS)
    # =========================================================================
    
    def get_customer_ecdd_history(
        self, 
        customer_id: str, 
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get historical ECDD assessments for a customer.
        
        Enables periodic review comparisons by returning all past
        assessments sorted by date (newest first).
        
        Args:
            customer_id: Customer ID to look up
            limit: Maximum number of records to return
            
        Returns:
            List of ECDD output records
        """
        id_col = self.config.output_columns.get("customer_id", "case_id")
        created_col = self.config.output_columns.get("created_at", "created_at")
        
        query = f"""
            SELECT * FROM {self.config.output_full_table}
            WHERE {id_col} = '{customer_id}'
            ORDER BY {created_col} DESC
            LIMIT {limit}
        """
        
        try:
            results = self._execute_query(query)
            
            # Parse JSON columns back into structured data
            parsed_results = []
            for row in results:
                parsed = dict(row)
                
                # Parse nested JSON columns
                for json_col in ['compliance_flags_json', 'ecdd_assessment_json', 
                                 'document_checklist_json', 'questionnaire_responses_json']:
                    if json_col in parsed and parsed[json_col]:
                        try:
                            parsed[json_col.replace('_json', '')] = json.loads(parsed[json_col])
                        except:
                            pass
                
                parsed_results.append(parsed)
            
            logger.info(f"Retrieved {len(parsed_results)} ECDD records for customer {customer_id}")
            return parsed_results
            
        except Exception as e:
            logger.error(f"Error retrieving customer history: {e}")
            # Fallback to local files
            return self._get_customer_history_local(customer_id, limit)
    
    def _get_customer_history_local(
        self, 
        customer_id: str, 
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Fallback: Get customer history from local JSON files."""
        results = []
        
        if not os.path.exists(self.config.local_fallback_path):
            return results
        
        for filename in os.listdir(self.config.local_fallback_path):
            if filename.endswith('_output.json'):
                filepath = os.path.join(self.config.local_fallback_path, filename)
                try:
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                    if data.get('customer_id') == customer_id:
                        results.append(data)
                except:
                    continue
        
        # Sort by created_at descending
        results.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return results[:limit]
    
    def get_latest_ecdd_for_customer(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the most recent ECDD assessment for a customer.
        
        Args:
            customer_id: Customer ID
            
        Returns:
            Latest ECDD record or None
        """
        history = self.get_customer_ecdd_history(customer_id, limit=1)
        return history[0] if history else None
    
    # =========================================================================
    # CONNECTION MANAGEMENT
    # =========================================================================
    
    def close(self):
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            self._is_connected = False
            logger.info("Closed Databricks connection")
    
    def is_connected(self) -> bool:
        """Check if connected to Databricks."""
        return self._is_connected
    
    def test_connection(self) -> bool:
        """Test the Databricks connection."""
        try:
            self._get_connection()
            return True
        except:
            return False


# =============================================================================
# SINGLETON ACCESSOR
# =============================================================================

_connector_instance: Optional[DatabricksConnector] = None


def get_databricks_connector(
    config: DatabricksConfig = None,
    profile_config: ProfileConfig = None
) -> DatabricksConnector:
    """Get singleton Databricks connector instance."""
    global _connector_instance
    if _connector_instance is None:
        _connector_instance = DatabricksConnector(config, profile_config)
    return _connector_instance
