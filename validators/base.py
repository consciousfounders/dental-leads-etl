"""
Base Validator Classes

Abstract interfaces for all validators (address, phone, email).
Allows easy swapping of vendor implementations without changing pipeline code.

Benefits:
- Vendor-agnostic pipeline code
- Easy A/B testing of validators
- Consensus validation (run multiple validators, compare results)
- SOC 2 audit trail (all calls logged)
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import time


class BaseValidator(ABC):
    """
    Abstract base class for all validators.
    
    All validator implementations must inherit from this class
    and implement the validate() method.
    """
    
    def __init__(self, api_key: str):
        """
        Initialize validator with API key.
        
        Args:
            api_key: API key for the validation service
        """
        self.api_key = api_key
        self.service_name = self.__class__.__name__
    
    @abstractmethod
    def validate(self, **kwargs) -> Dict[str, Any]:
        """
        Validate input and return standardized result.
        
        All validators must return a dict with at least:
        {
            'valid': bool,
            'confidence': float (0.0 to 1.0),
            'provider': str (name of validation service),
            'timestamp': str (ISO format),
            'response_time_ms': float
        }
        
        Additional fields are validator-specific.
        """
        pass
    
    def _log_call(self, endpoint: str, status_code: int, response_time_ms: float, error: Optional[str] = None):
        """
        Log API call for audit trail.
        
        This is called automatically by validators to maintain SOC 2 compliance.
        """
        from utils.audit_logger import AuditLogger
        
        AuditLogger.log_api_call(
            service=self.service_name,
            endpoint=endpoint,
            status_code=status_code,
            response_time_ms=response_time_ms,
            error=error
        )
    
    def _time_call(self, func, *args, **kwargs):
        """
        Execute function and measure response time.
        
        Returns:
            (result, response_time_ms)
        """
        start_time = time.time()
        result = func(*args, **kwargs)
        response_time_ms = (time.time() - start_time) * 1000
        return result, response_time_ms


class AddressValidator(BaseValidator):
    """
    Abstract base class for address validators.
    
    Implementations: Addy, SmartyStreets, Melissa Data, USPS, etc.
    """
    
    @abstractmethod
    def validate(
        self,
        address_line_1: str,
        city: str,
        state: str,
        zip_code: str,
        address_line_2: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate and normalize address.
        
        Args:
            address_line_1: Street address
            city: City name
            state: State abbreviation (e.g., 'CA')
            zip_code: ZIP code
            address_line_2: Optional apartment, suite, etc.
        
        Returns:
            {
                'valid': bool,
                'confidence': float,
                'normalized_address': str,
                'delivery_line_1': str,
                'delivery_line_2': str,
                'city': str,
                'state': str,
                'zip': str,
                'zip4': str,
                'provider': str,
                'timestamp': str,
                'response_time_ms': float
            }
        """
        pass


class PhoneValidator(BaseValidator):
    """
    Abstract base class for phone validators.
    
    Implementations: Twilio, Telnyx, Plivo, etc.
    """
    
    @abstractmethod
    def validate(self, phone_number: str, country_code: str = "US") -> Dict[str, Any]:
        """
        Validate phone number.
        
        Args:
            phone_number: Phone number to validate
            country_code: Country code (default: US)
        
        Returns:
            {
                'valid': bool,
                'confidence': float,
                'formatted_number': str,
                'line_type': str,  # mobile, landline, voip
                'carrier': str,
                'country_code': str,
                'provider': str,
                'timestamp': str,
                'response_time_ms': float
            }
        """
        pass


class EmailValidator(BaseValidator):
    """
    Abstract base class for email validators.
    
    Implementations: NeverBounce, ZeroBounce, Kickbox, etc.
    """
    
    @abstractmethod
    def validate(self, email: str) -> Dict[str, Any]:
        """
        Validate email address.
        
        Args:
            email: Email address to validate
        
        Returns:
            {
                'valid': bool,
                'confidence': float,
                'deliverable': bool,
                'role_account': bool,  # info@, sales@, etc.
                'disposable': bool,
                'mx_found': bool,
                'smtp_check': bool,
                'provider': str,
                'timestamp': str,
                'response_time_ms': float
            }
        """
        pass


# Example usage:
"""
from validators.address_validator import AddyValidator

validator = AddyValidator(api_key="your_api_key")
result = validator.validate(
    address_line_1="123 Main St",
    city="San Francisco",
    state="CA",
    zip_code="94103"
)

print(result['valid'])  # True/False
print(result['confidence'])  # 0.0 to 1.0
print(result['normalized_address'])  # "123 MAIN ST"
"""
