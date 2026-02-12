# utils/service_utils.py
import os

from typing import Dict

# -------------------------------------------------
# VALIDATE FORM DATA
# -------------------------------------------------
def validate_form_content(service_content: Dict[str, str]) -> tuple[bool, str]:
    """
    Validate that required fields are provided
    
    Returns:
        (is_valid, error_message)
    """
    
    required_fields = {
        "service_name": "Service name is required",
        "service_description": "Service description is required",
        "how_to_apply": "Application process (how to apply) is required",
        "eligibility_criteria": "Eligibility criteria is required",
        "required_docs": "Required documents list is required"
    }
    
    for field, error_msg in required_fields.items():
        if not service_content.get(field, "").strip():
            return False, error_msg
    
    return True, "Valid"


