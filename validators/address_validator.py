"""
Addy Address Validator Implementation

Validates addresses using Addy's USPS validation API.
Addy provides USPS CASS-certified address validation.

API Docs: https://docs.addy.co
"""

import requests
from typing import Dict, Any, Optional
from datetime import datetime
from validators.base import AddressValidator


class AddyValidator(AddressValidator):
    """
    Addy address validator implementation.
    
    Features:
    - USPS CASS-certified validation
    - Address normalization to USPS format
    - Confidence scoring
    - Delivery point validation
    """
    
    BASE_URL = "https://api.addy.co/v1"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Addy validator.
        
        Args:
            api_key: Addy API key (fetched from secrets if not provided)
        """
        if api_key is None:
            from utils.secrets_manager import get_secrets_manager
            secrets = get_secrets_manager()
            api_key = secrets.get_secret("addy-api-key")
        
        super().__init__(api_key)
    
    def validate(
        self,
        address_line_1: str,
        city: str,
        state: str,
        zip_code: str,
        address_line_2: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate address using Addy API.
        
        Returns standardized result with USPS-normalized address.
        """
        # Build request payload
        payload = {
            "address1": address_line_1,
            "city": city,
            "state": state,
            "zip": zip_code
        }
        
        if address_line_2:
            payload["address2"] = address_line_2
        
        # Make API call with timing
        try:
            response, response_time_ms = self._time_call(
                self._call_api,
                payload
            )
            
            # Log successful call
            self._log_call(
                endpoint="/v1/validate",
                status_code=200,
                response_time_ms=response_time_ms
            )
            
            # Parse response
            return self._parse_response(response, response_time_ms)
            
        except requests.exceptions.RequestException as e:
            # Log failed call
            self._log_call(
                endpoint="/v1/validate",
                status_code=getattr(e.response, 'status_code', None),
                response_time_ms=0,
                error=str(e)
            )
            
            # Return error result
            return self._error_response(str(e))
    
    def _call_api(self, payload: Dict[str, str]) -> Dict[str, Any]:
        """Make actual API call to Addy"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            f"{self.BASE_URL}/validate",
            json=payload,
            headers=headers,
            timeout=10  # 10 second timeout
        )
        
        response.raise_for_status()
        return response.json()
    
    def _parse_response(self, response: Dict[str, Any], response_time_ms: float) -> Dict[str, Any]:
        """
        Parse Addy API response into standardized format.
        
        Addy response format:
        {
            "status": "valid" | "invalid" | "unknown",
            "deliverable": true/false,
            "address": {
                "delivery_line_1": "123 MAIN ST",
                "delivery_line_2": "",
                "city": "SAN FRANCISCO",
                "state": "CA",
                "zip": "94103",
                "zip4": "1234"
            },
            "metadata": {
                "dpv_match": "Y",
                "dpv_footnotes": ["AA", "BB"],
                "confidence": 0.98
            }
        }
        """
        status = response.get("status", "unknown")
        address_data = response.get("address", {})
        metadata = response.get("metadata", {})
        
        # Build normalized address string
        normalized_parts = [
            address_data.get("delivery_line_1", ""),
            address_data.get("delivery_line_2", ""),
        ]
        normalized_address = " ".join(filter(None, normalized_parts))
        
        return {
            "valid": status == "valid" and response.get("deliverable", False),
            "confidence": metadata.get("confidence", 0.0),
            "normalized_address": normalized_address,
            "delivery_line_1": address_data.get("delivery_line_1", ""),
            "delivery_line_2": address_data.get("delivery_line_2", ""),
            "city": address_data.get("city", ""),
            "state": address_data.get("state", ""),
            "zip": address_data.get("zip", ""),
            "zip4": address_data.get("zip4", ""),
            "dpv_match": metadata.get("dpv_match"),  # Delivery Point Validation
            "dpv_footnotes": metadata.get("dpv_footnotes", []),
            "provider": "Addy",
            "timestamp": datetime.utcnow().isoformat(),
            "response_time_ms": response_time_ms
        }
    
    def _error_response(self, error_message: str) -> Dict[str, Any]:
        """Return standardized error response"""
        return {
            "valid": False,
            "confidence": 0.0,
            "normalized_address": "",
            "delivery_line_1": "",
            "delivery_line_2": "",
            "city": "",
            "state": "",
            "zip": "",
            "zip4": "",
            "dpv_match": None,
            "dpv_footnotes": [],
            "provider": "Addy",
            "timestamp": datetime.utcnow().isoformat(),
            "response_time_ms": 0,
            "error": error_message
        }


# Example usage:
"""
from validators.address_validator import AddyValidator

validator = AddyValidator()  # API key fetched from Secret Manager

result = validator.validate(
    address_line_1="1600 Amphitheatre Parkway",
    city="Mountain View",
    state="CA",
    zip_code="94043"
)

if result['valid']:
    print(f"✅ Valid address: {result['normalized_address']}")
    print(f"   Confidence: {result['confidence']:.1%}")
    print(f"   ZIP+4: {result['zip']}-{result['zip4']}")
else:
    print(f"❌ Invalid address")
    print(f"   Error: {result.get('error', 'Unknown')}")
"""
