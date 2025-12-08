"""
Twilio Phone Validator Implementation

Validates phone numbers using Twilio Lookup API.
Provides line type, carrier, and validity information.

API Docs: https://www.twilio.com/docs/lookup
"""

import requests
from typing import Dict, Any, Optional
from datetime import datetime
from validators.base import PhoneValidator


class TwilioValidator(PhoneValidator):
    """
    Twilio phone validator implementation.
    
    Features:
    - Phone number validity check
    - Line type detection (mobile, landline, voip)
    - Carrier lookup
    - International number support
    """
    
    BASE_URL = "https://lookups.twilio.com/v2"
    
    def __init__(
        self,
        account_sid: Optional[str] = None,
        auth_token: Optional[str] = None
    ):
        """
        Initialize Twilio validator.
        
        Args:
            account_sid: Twilio Account SID (fetched from secrets if not provided)
            auth_token: Twilio Auth Token (fetched from secrets if not provided)
        """
        if account_sid is None or auth_token is None:
            from utils.secrets_manager import get_secrets_manager
            secrets = get_secrets_manager()
            account_sid = secrets.get_secret("twilio-account-sid")
            auth_token = secrets.get_secret("twilio-auth-token")
        
        self.account_sid = account_sid
        super().__init__(auth_token)  # auth_token is the "api_key"
    
    def validate(
        self,
        phone_number: str,
        country_code: str = "US"
    ) -> Dict[str, Any]:
        """
        Validate phone number using Twilio Lookup API.
        
        Args:
            phone_number: Phone number to validate (can be in any format)
            country_code: Country code (default: US)
        
        Returns:
            Standardized validation result with line type and carrier info
        """
        # Normalize phone number format
        normalized_phone = self._normalize_phone(phone_number, country_code)
        
        # Make API call with timing
        try:
            response, response_time_ms = self._time_call(
                self._call_api,
                normalized_phone
            )
            
            # Log successful call
            self._log_call(
                endpoint=f"/v2/PhoneNumbers/{normalized_phone}",
                status_code=200,
                response_time_ms=response_time_ms
            )
            
            # Parse response
            return self._parse_response(response, response_time_ms)
            
        except requests.exceptions.RequestException as e:
            # Log failed call
            self._log_call(
                endpoint=f"/v2/PhoneNumbers/{normalized_phone}",
                status_code=getattr(e.response, 'status_code', None),
                response_time_ms=0,
                error=str(e)
            )
            
            # Return error result
            return self._error_response(phone_number, str(e))
    
    def _normalize_phone(self, phone_number: str, country_code: str) -> str:
        """
        Normalize phone number to E.164 format.
        
        Examples:
            (415) 555-1234 -> +14155551234
            415-555-1234 -> +14155551234
        """
        # Remove all non-digit characters
        digits = ''.join(filter(str.isdigit, phone_number))
        
        # Add country code if not present
        if not phone_number.startswith('+'):
            if country_code == "US" and len(digits) == 10:
                return f"+1{digits}"
            elif len(digits) == 11 and digits[0] == '1':
                return f"+{digits}"
            else:
                return f"+{digits}"
        
        return phone_number
    
    def _call_api(self, phone_number: str) -> Dict[str, Any]:
        """Make actual API call to Twilio Lookup API"""
        # Twilio uses basic auth (account_sid:auth_token)
        auth = (self.account_sid, self.api_key)
        
        # Request line type and carrier information
        params = {
            "Fields": "line_type_intelligence,carrier"
        }
        
        response = requests.get(
            f"{self.BASE_URL}/PhoneNumbers/{phone_number}",
            auth=auth,
            params=params,
            timeout=10
        )
        
        response.raise_for_status()
        return response.json()
    
    def _parse_response(
        self,
        response: Dict[str, Any],
        response_time_ms: float
    ) -> Dict[str, Any]:
        """
        Parse Twilio API response into standardized format.
        
        Twilio response format:
        {
            "phone_number": "+14155551234",
            "national_format": "(415) 555-1234",
            "country_code": "US",
            "valid": true,
            "line_type_intelligence": {
                "type": "mobile",
                "carrier_name": "Verizon"
            },
            "carrier": {
                "name": "Verizon",
                "type": "mobile"
            }
        }
        """
        line_type_info = response.get("line_type_intelligence", {})
        carrier_info = response.get("carrier", {})
        
        # Determine line type
        line_type = line_type_info.get("type") or carrier_info.get("type", "unknown")
        
        # Get carrier name
        carrier = (
            line_type_info.get("carrier_name") or
            carrier_info.get("name") or
            "unknown"
        )
        
        return {
            "valid": response.get("valid", False),
            "confidence": 1.0 if response.get("valid") else 0.0,
            "formatted_number": response.get("national_format", ""),
            "e164_format": response.get("phone_number", ""),
            "line_type": line_type,
            "carrier": carrier,
            "country_code": response.get("country_code", ""),
            "provider": "Twilio",
            "timestamp": datetime.utcnow().isoformat(),
            "response_time_ms": response_time_ms
        }
    
    def _error_response(self, phone_number: str, error_message: str) -> Dict[str, Any]:
        """Return standardized error response"""
        return {
            "valid": False,
            "confidence": 0.0,
            "formatted_number": phone_number,
            "e164_format": "",
            "line_type": "unknown",
            "carrier": "unknown",
            "country_code": "",
            "provider": "Twilio",
            "timestamp": datetime.utcnow().isoformat(),
            "response_time_ms": 0,
            "error": error_message
        }


# Example usage:
"""
from validators.phone_validator import TwilioValidator

validator = TwilioValidator()  # Credentials fetched from Secret Manager

result = validator.validate("(415) 555-1234")

if result['valid']:
    print(f"✅ Valid phone number: {result['formatted_number']}")
    print(f"   Line type: {result['line_type']}")
    print(f"   Carrier: {result['carrier']}")
else:
    print(f"❌ Invalid phone number")
    print(f"   Error: {result.get('error', 'Unknown')}")
"""
