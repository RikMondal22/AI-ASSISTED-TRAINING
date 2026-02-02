"""
PDF Content Validator
Ensures PDF contains required government service information
"""

import re
from typing import Dict, Tuple, List
import google.genai as genai
from config import GOOGLE_API_KEY, GEMINI_MODEL

# -------------------------------------------------
# REQUIRED FIELDS FOR GOVERNMENT SERVICE PDF
# -------------------------------------------------
REQUIRED_FIELDS = {
    "service_name": "Service Name/Title",
    "service_description": "Service Description/Overview/Purpose",
    "how_to_apply": "Application Process/How to Apply/Steps",
    "eligibility_criteria": "Eligibility/Who can apply/Requirements",
    "required_documents": "Required Documents/Document List/Papers needed"
}

# -------------------------------------------------
# INITIALIZE GEMINI CLIENT
# -------------------------------------------------
if GOOGLE_API_KEY:
    client = genai.Client(api_key=GOOGLE_API_KEY)
else:
    client = None
    print("⚠️ GOOGLE_API_KEY not found. Advanced validation disabled.")


# -------------------------------------------------
# BASIC KEYWORD VALIDATION (FAST CHECK)
# -------------------------------------------------
def quick_keyword_check(raw_text: str) -> Dict[str, bool]:
    """
    Fast regex-based check for required field indicators
    
    Returns:
        Dictionary of field: found_status
    """
    text_lower = raw_text.lower()
    
    field_patterns = {
        "service_name": r"(service\s+name|title|scheme\s+name|program\s+name)",
        "service_description": r"(description|overview|purpose|about|introduction)",
        "how_to_apply": r"(how\s+to\s+apply|application\s+process|procedure|steps|method)",
        "eligibility_criteria": r"(eligibility|who\s+can\s+apply|criteria|requirements|qualification)",
        "required_documents": r"(required\s+documents|documents?\s+needed|papers?\s+required|document\s+list)"
    }
    
    results = {}
    for field, pattern in field_patterns.items():
        results[field] = bool(re.search(pattern, text_lower))
    
    return results


# -------------------------------------------------
# AI-BASED VALIDATION (DEEP CHECK)
# -------------------------------------------------
def ai_validate_pdf_content(raw_text: str) -> Tuple[bool, str, Dict[str, bool]]:
    """
    Use Gemini AI to analyze if PDF contains government service information
    
    Returns:
        (is_valid, error_message, field_detection_dict)
    """
    
    if not client:
        # Fallback to keyword check if AI unavailable
        keyword_results = quick_keyword_check(raw_text)
        missing = [field for field, found in keyword_results.items() if not found]
        
        if len(missing) > 2:  # Allow some flexibility
            return False, f"PDF appears to be missing: {', '.join(missing)}", keyword_results
        return True, "", keyword_results
    
    # Build validation prompt
    prompt = f"""
You are a document classifier for government service training systems.

TASK: Analyze if this document contains information about a GOVERNMENT SERVICE or PUBLIC SCHEME.

REQUIRED INFORMATION (must contain at least 4 out of 5):
1. Service Name/Title - What is the service called?
2. Service Description - What does this service do? Who benefits?
3. Application Process - How do citizens apply? What are the steps?
4. Eligibility Criteria - Who can apply? What are the requirements?
5. Required Documents - What documents are needed to apply?

DOCUMENT TEXT:
{raw_text[:3000]}  

OUTPUT FORMAT (JSON only):
{{
  "is_government_service": true/false,
  "document_type": "government service" or "CV/resume" or "other",
  "confidence": 0.0-1.0,
  "fields_found": {{
    "service_name": true/false,
    "service_description": true/false,
    "how_to_apply": true/false,
    "eligibility_criteria": true/false,
    "required_documents": true/false
  }},
  "reason": "Brief explanation why this is/isn't a government service document"
}}

EXAMPLES:
- Government service PDF: Ration Card application, Pension scheme, Birth certificate
- NOT government service: Personal CV, research paper, novel, invoice
"""
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )
        
        # Extract JSON from response
        import json
        match = re.search(r'\{[\s\S]*\}', response.text)
        if not match:
            raise ValueError("No JSON in AI response")
        
        result = json.loads(match.group())
        
        # Validate response structure
        is_valid = result.get("is_government_service", False)
        confidence = result.get("confidence", 0.0)
        fields_found = result.get("fields_found", {})
        reason = result.get("reason", "Unknown")
        
        # Check if enough fields are present
        found_count = sum(fields_found.values())
        
        if not is_valid or found_count < 3:  # At least 3 fields required
            missing_fields = [field for field, found in fields_found.items() if not found]
            error_msg = f"Invalid PDF: {reason}. Missing fields: {', '.join(missing_fields)}"
            return False, error_msg, fields_found
        
        return True, "", fields_found
        
    except Exception as e:
        print(f"⚠️ AI validation failed: {e}. Falling back to keyword check.")
        # Fallback to keyword validation
        keyword_results = quick_keyword_check(raw_text)
        missing = [field for field, found in keyword_results.items() if not found]
        
        if len(missing) > 2:
            return False, f"PDF validation failed. Possibly missing: {', '.join(missing)}", keyword_results
        return True, "", keyword_results


# -------------------------------------------------
# MAIN VALIDATION FUNCTION
# -------------------------------------------------
def validate_pdf_content(pdf_pages: List[Dict]) -> Tuple[bool, str]:
    """
    Validate that PDF contains required government service fields
    
    Args:
        pdf_pages: Output from extract_raw_content()
        
    Returns:
        (is_valid, error_message)
    """
    
    # Combine all page text
    all_text = "\n".join(
        " ".join(page["lines"]) 
        for page in pdf_pages
    )
    
    # Check minimum content length
    if len(all_text.strip()) < 100:
        return False, "PDF contains insufficient text. Please provide a detailed government service document."
    
    # Run AI validation
    is_valid, error_msg, fields_found = ai_validate_pdf_content(all_text)
    
    if not is_valid:
        return False, error_msg
    
    # Additional sanity check
    if len(all_text.strip()) < 500:
        return False, "PDF content too short for a complete government service description."
    
    return True, "PDF validation passed"


# -------------------------------------------------
# TESTING
# -------------------------------------------------
if __name__ == "__main__":
    from pdf_extractor import extract_raw_content
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python pdf_validator.py <path_to_pdf>")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    
    print(f"\n📄 Validating PDF: {pdf_path}")
    
    # Extract content
    pages = extract_raw_content(pdf_path)
    
    # Validate
    is_valid, message = validate_pdf_content(pages)
    
    if is_valid:
        print(f"✅ {message}")
    else:
        print(f"❌ {message}")
        sys.exit(1)