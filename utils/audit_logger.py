"""
Audit Logger for SOC 2 Compliance

Logs all:
- External API calls (Addy, Twilio, enrichment providers)
- Data access events
- Secret access (via SecretsManager)
- System events

Requirements for SOC 2:
- Immutable log entries
- Centralized logging
- Retention policy (typically 1+ years)
- Searchable and auditable
"""

import logging
import json
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

# Configure logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of auditable events"""
    API_CALL = "API_CALL"
    DATA_ACCESS = "DATA_ACCESS"
    SECRET_ACCESS = "SECRET_ACCESS"
    SYSTEM_EVENT = "SYSTEM_EVENT"
    ERROR = "ERROR"


class AuditLogger:
    """
    Centralized audit logging for SOC 2 compliance.
    
    In production, these logs should be:
    1. Written to Cloud Logging (GCP)
    2. Exported to BigQuery for long-term retention
    3. Protected with IAM (read-only for auditors)
    4. Retained per policy (1+ years)
    """
    
    @staticmethod
    def log_api_call(
        service: str,
        endpoint: str,
        method: str = "POST",
        status_code: Optional[int] = None,
        response_time_ms: Optional[float] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log external API call for audit trail.
        
        Args:
            service: API service name (e.g., 'Addy', 'Twilio', 'Wiza')
            endpoint: API endpoint called
            method: HTTP method (GET, POST, etc.)
            status_code: HTTP response code
            response_time_ms: Response time in milliseconds
            error: Error message if call failed
            metadata: Additional context (request ID, user ID, etc.)
        """
        log_entry = {
            "event_type": EventType.API_CALL.value,
            "timestamp": datetime.utcnow().isoformat(),
            "service": service,
            "endpoint": endpoint,
            "method": method,
            "status_code": status_code,
            "response_time_ms": response_time_ms,
            "success": status_code is not None and 200 <= status_code < 300,
            "error": error,
            "metadata": metadata or {}
        }
        
        # Log at appropriate level
        if error or (status_code and status_code >= 400):
            logger.error(f"API_CALL: {json.dumps(log_entry)}")
        else:
            logger.info(f"API_CALL: {json.dumps(log_entry)}")
    
    @staticmethod
    def log_data_access(
        table: str,
        operation: str,
        row_count: Optional[int] = None,
        user: Optional[str] = None,
        success: bool = True,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log data access event (read/write to database).
        
        Args:
            table: Table name accessed
            operation: Operation type (SELECT, INSERT, UPDATE, DELETE)
            row_count: Number of rows affected
            user: User or service account performing operation
            success: Whether operation succeeded
            error: Error message if failed
            metadata: Additional context
        """
        log_entry = {
            "event_type": EventType.DATA_ACCESS.value,
            "timestamp": datetime.utcnow().isoformat(),
            "table": table,
            "operation": operation,
            "row_count": row_count,
            "user": user,
            "success": success,
            "error": error,
            "metadata": metadata or {}
        }
        
        if error or not success:
            logger.error(f"DATA_ACCESS: {json.dumps(log_entry)}")
        else:
            logger.info(f"DATA_ACCESS: {json.dumps(log_entry)}")
    
    @staticmethod
    def log_system_event(
        event_name: str,
        description: str,
        severity: str = "INFO",
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log system event (pipeline start/stop, configuration change, etc.).
        
        Args:
            event_name: Name of the event
            description: Human-readable description
            severity: Event severity (INFO, WARNING, ERROR, CRITICAL)
            metadata: Additional context
        """
        log_entry = {
            "event_type": EventType.SYSTEM_EVENT.value,
            "timestamp": datetime.utcnow().isoformat(),
            "event_name": event_name,
            "description": description,
            "severity": severity,
            "metadata": metadata or {}
        }
        
        severity_lower = severity.lower()
        if severity_lower == "error":
            logger.error(f"SYSTEM_EVENT: {json.dumps(log_entry)}")
        elif severity_lower == "warning":
            logger.warning(f"SYSTEM_EVENT: {json.dumps(log_entry)}")
        elif severity_lower == "critical":
            logger.critical(f"SYSTEM_EVENT: {json.dumps(log_entry)}")
        else:
            logger.info(f"SYSTEM_EVENT: {json.dumps(log_entry)}")
    
    @staticmethod
    def log_error(
        error_type: str,
        error_message: str,
        stack_trace: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log error event.
        
        Args:
            error_type: Type of error (e.g., 'ValidationError', 'DatabaseError')
            error_message: Error message
            stack_trace: Full stack trace
            metadata: Additional context
        """
        log_entry = {
            "event_type": EventType.ERROR.value,
            "timestamp": datetime.utcnow().isoformat(),
            "error_type": error_type,
            "error_message": error_message,
            "stack_trace": stack_trace,
            "metadata": metadata or {}
        }
        
        logger.error(f"ERROR: {json.dumps(log_entry)}")


# Example usage:
"""
from utils.audit_logger import AuditLogger

# Log API call
AuditLogger.log_api_call(
    service="Addy",
    endpoint="/v1/validate",
    status_code=200,
    response_time_ms=145.3
)

# Log data access
AuditLogger.log_data_access(
    table="RAW.NPI_PROVIDERS",
    operation="INSERT",
    row_count=1000,
    success=True
)

# Log system event
AuditLogger.log_system_event(
    event_name="PIPELINE_START",
    description="NPI ingestion pipeline started",
    metadata={"pipeline": "npi_ingestion"}
)
"""
