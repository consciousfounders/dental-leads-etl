"""
Secrets Manager with GCP Secret Manager + Environment Variable Fallback

Flow:
1. Try GCP Secret Manager (production)
2. Fall back to environment variables (local dev)
3. Raise error if neither available

SOC 2 Compliance:
- All secret accesses are logged
- Supports audit trail through GCP logging
- No plaintext secrets in code
"""

import os
import logging
from typing import Optional
from datetime import datetime

# Conditional import - allows env var fallback when Secret Manager not installed
try:
    from google.cloud import secretmanager
    SECRET_MANAGER_AVAILABLE = True
except ImportError:
    secretmanager = None
    SECRET_MANAGER_AVAILABLE = False

logger = logging.getLogger(__name__)


class SecretsManager:
    """
    Unified secrets management supporting:
    - GCP Secret Manager (production)
    - Environment variables (local dev/fallback)
    """
    
    def __init__(self):
        self.gcp_project = os.getenv("GCP_PROJECT_ID")
        self.client = None
        self.use_secret_manager = False
        
        # Check if Secret Manager is explicitly disabled
        skip_secret_manager = os.getenv("SKIP_SECRET_MANAGER", "").lower() in ("true", "1", "yes")
        
        if skip_secret_manager:
            logger.info("ℹ️  SKIP_SECRET_MANAGER=true - using local secrets only")
        # Try to initialize Secret Manager (only if package is available)
        elif not SECRET_MANAGER_AVAILABLE:
            logger.info("ℹ️  google-cloud-secret-manager not installed - using environment variables")
        elif self.gcp_project:
            try:
                self.client = secretmanager.SecretManagerServiceClient()
                self.use_secret_manager = True
                logger.info("✅ GCP Secret Manager initialized")
            except Exception as e:
                logger.warning(f"⚠️  GCP Secret Manager unavailable: {e}")
                logger.warning("   Falling back to environment variables")
        else:
            logger.info("ℹ️  GCP_PROJECT_ID not set - using environment variables")
    
    def get_secret(self, secret_name: str, required: bool = True) -> Optional[str]:
        """
        Fetch secret with automatic fallback.
        
        Args:
            secret_name: Name of the secret (e.g., 'snowflake-account')
            required: If True, raises error when secret not found
        
        Returns:
            Secret value as string, or None if not required and not found
        
        Raises:
            ValueError: If secret not found and required=True
        """
        # Log the access attempt (SOC 2 audit trail)
        self._log_access(secret_name, "ATTEMPT")
        
        # Try GCP Secret Manager first
        if self.use_secret_manager and self.client:
            try:
                secret_value = self._get_from_secret_manager(secret_name)
                if secret_value:
                    self._log_access(secret_name, "SUCCESS", source="SECRET_MANAGER")
                    return secret_value
            except Exception as e:
                logger.warning(f"⚠️  Failed to fetch '{secret_name}' from Secret Manager: {e}")
                self._log_access(secret_name, "FAILED", source="SECRET_MANAGER", error=str(e))
        
        # Fall back to environment variable
        env_var_name = secret_name.upper().replace("-", "_")
        env_value = os.getenv(env_var_name)
        
        if env_value:
            self._log_access(secret_name, "SUCCESS", source="ENVIRONMENT")
            return env_value
        
        # Secret not found
        self._log_access(secret_name, "NOT_FOUND")
        
        if required:
            raise ValueError(
                f"Secret '{secret_name}' not found in Secret Manager or environment variable '{env_var_name}'"
            )
        
        return None
    
    def _get_from_secret_manager(self, secret_name: str) -> str:
        """Fetch secret from GCP Secret Manager"""
        name = f"projects/{self.gcp_project}/secrets/{secret_name}/versions/latest"
        response = self.client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    
    def _log_access(
        self, 
        secret_name: str, 
        status: str, 
        source: Optional[str] = None,
        error: Optional[str] = None
    ):
        """
        Log secret access for audit trail (SOC 2 compliance)
        
        This creates an audit trail of all secret accesses, which is critical
        for SOC 2 compliance. In production, these logs should be:
        - Centralized (e.g., Cloud Logging)
        - Immutable
        - Retained per policy (typically 1+ years)
        """
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "secret_name": secret_name,
            "status": status,
            "source": source,
            "error": error
        }
        
        # Log at appropriate level
        if status == "SUCCESS":
            logger.info(f"SECRET_ACCESS: {log_entry}")
        elif status == "NOT_FOUND":
            logger.error(f"SECRET_ACCESS: {log_entry}")
        else:
            logger.warning(f"SECRET_ACCESS: {log_entry}")


# Singleton instance for easy import
_secrets_manager = None

def get_secrets_manager() -> SecretsManager:
    """Get or create singleton SecretsManager instance"""
    global _secrets_manager
    if _secrets_manager is None:
        _secrets_manager = SecretsManager()
    return _secrets_manager
