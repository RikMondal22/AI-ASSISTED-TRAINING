# """
# Gemini slide generator
# RAW PDF text → CLEAN SLIDES (STRICT FORMAT)
# """

# import google.genai as genai
# import json
# import re
# from config import GOOGLE_API_KEY, GEMINI_MODEL

# # -------------------------------------------------
# # CONFIG
# # -------------------------------------------------
# if not GOOGLE_API_KEY:
#     raise ValueError(
#         "GOOGLE_API_KEY not found in environment. "
#         "Please set it in your .env file or environment variables."
#     )

# client = genai.Client(api_key=GOOGLE_API_KEY)
"""
Gemini Form Content Processor
Enhances raw form data into professional training slides
"""

import google.genai as genai
import json
import re
from typing import Dict, List
from config import GOOGLE_API_KEY, GEMINI_MODEL

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
if not GOOGLE_API_KEY:
    raise ValueError(
        "GOOGLE_API_KEY not found in environment. "
        "Please set it in your .env file or environment variables."
    )

client = genai.Client(api_key=GOOGLE_API_KEY)

# -------------------------------------------------
# SAFE JSON EXTRACTOR
# -------------------------------------------------
# def extract_json(text: str):
#     match = re.search(r"\{[\s\S]*\}", text)
#     if not match:
#         raise ValueError("No JSON found in Gemini response")
#     return json.loads(match.group())
def extract_json(text: str):
    """Extract JSON from Gemini response"""
    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        raise ValueError("No JSON found in Gemini response")
    return json.loads(match.group())


# -------------------------------------------------
# FORM CONTENT ENHANCEMENT PROMPT
# -------------------------------------------------
def build_form_enhancement_prompt(service_content: Dict[str, str]) -> str:
    """Build prompt to enhance form content into professional training slides"""
    
    return f"""
You are creating professional PowerPoint slides for BSK (Bangla Sahayta Kendra) training videos.
These videos train Data Entry Operators on how to help citizens access government services.

INPUT DATA (from BSK portal form):
Service Name: {service_content['service_name']}
Service Description: {service_content['service_description']}
How to Apply: {service_content['how_to_apply']}
Eligibility Criteria: {service_content['eligibility_criteria']}
Required Documents: {service_content['required_docs']}
Fees & Timeline: {service_content.get('fees_and_timeline', 'Not provided')}
Operator Tips: {service_content.get('operator_tips', 'Not provided')}
Troubleshooting: {service_content.get('troubleshooting', 'Not provided')}
Service Link: {service_content.get('service_link', 'Not provided')}

TASK:
Transform this raw form data into clean, professional training slides for BSK operators.

STRICT SLIDE RULES:
1. Create EXACTLY these slides (in this order):
   - Slide 1: Service Introduction
   - Slide 2: Eligibility Criteria
   - Slide 3: Required Documents
   - Slide 4: Application Process (step-by-step)
   - Slide 5: Fees & Timeline (if data provided)
   - Slide 6: Professional Tips for Operators (if data provided)
   - Slide 7: Common Issues & Solutions (if data provided)
   - Slide 8: Online Service Access (if link provided)
   - Slide 9: Training Complete (conclusion)

2. SKIP slides 5-8 if their corresponding data is "Not provided" or empty

3. For EACH slide:
   - Title: Maximum 6 words, clear and action-oriented
   - Bullets: 4-6 bullet points, maximum 12 words each
   - Write from operator's perspective (e.g., "Verify citizen's eligibility", "Help them prepare documents")
   - Use professional, training-oriented language
   - Be concise and actionable
   - Image keyword: 2-3 words that describe the slide concept

4. CONTENT ENHANCEMENT RULES:
   - Improve grammar and clarity
   - Make instructions more actionable
   - Add professional context where needed
   - Keep original meaning and facts
   - DO NOT invent information not in the input
   - If application steps are vague, structure them logically

5. OPERATOR FOCUS:
   - Remember: audience is BSK data entry operators
   - Use second-person ("You will help...", "Verify that...")
   - Include phrases like "Help citizens", "Guide them to", "Verify"
   - Make it clear this is training for operators, not citizens

OUTPUT FORMAT (JSON ONLY):
{{
  "slides": [
    {{
      "slide_no": 1,
      "title": "Service Introduction",
      "bullets": [
        "Welcome to training for [Service Name]",
        "You will help citizens access this important service",
        "[Concise description of what the service does]",
        "[Who this service benefits]"
      ],
      "image_keyword": "government training"
    }},
    {{
      "slide_no": 2,
      "title": "Eligibility Verification",
      "bullets": [
        "Before proceeding, verify citizens meet these criteria",
        "[Criterion 1]",
        "[Criterion 2]",
        "Ask clarifying questions if needed"
      ],
      "image_keyword": "verification checklist"
    }}
    // ... more slides following the structure above
  ]
}}

IMPORTANT:
- Use ONLY information from the input data
- Make content professional and training-ready
- Ensure logical flow from introduction to conclusion
- Each bullet should be clear and actionable
- Return ONLY valid JSON, no explanations
"""



# -------------------------------------------------
# PROMPT (STRICT OUTPUT CONTROL)
# -------------------------------------------------
def build_prompt(raw_text: str) -> str:
    return f"""
You are creating PowerPoint slides for a government training video.

TASK:
From the RAW TEXT below, create CLEAN, TRAINING-READY slides.

STRICT RULES (MANDATORY):
- Use ONLY information from the text
- Do NOT invent or assume information
- Generate ONLY the following slides if content exists:
  1. Service Overview
  2. Application Process
  3. Required Documents
  4. Eligibility Criteria
  5. Important Guidelines
  6. Fees & Timeline
  7. Tips for DEO Operators
  8. Common Troubleshooting
  9. Online Service Access
  10. Thank You / Conclusion
- ONE slide per topic (DO NOT split)
- Skip a slide if no information exists 
- Compress long procedures into concise bullets

SLIDE RULES:
- Title: max 6 words (use topic name)
- Bullets: 4–6 bullets, max 12 words each
- Image keyword: exactly 2–3 words

OUTPUT FORMAT (JSON ONLY – EXACT):
{{
  "slides": [
    {{
      "slide_no": 1,
      "title": "",
      "bullets": [],
      "image_keyword": ""
    }}
  ]
}}

RAW TEXT:
{raw_text}
"""


# -------------------------------------------------
# GENERATE SLIDES
# -------------------------------------------------
def generate_slides_from_raw(raw_text: str):
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=build_prompt(raw_text),
    )

    data = extract_json(response.text)

    # -------------------------------------------------
    # HARD SAFETY CHECK
    # -------------------------------------------------
    if "slides" not in data or not isinstance(data["slides"], list):
        raise ValueError("Invalid slide output from Gemini")

    # Re-number slides safely
    for i, slide in enumerate(data["slides"], start=1):
        slide["slide_no"] = i

    return data

# -------------------------------------------------
# GENERATE ENHANCED SLIDES FROM FORM
# -------------------------------------------------
def generate_slides_from_form(service_content: Dict[str, str]) -> Dict:
    """
    Process form content through Gemini to create professional slides
    
    Args:
        service_content: Dictionary with form fields
        
    Returns:
        Dictionary with 'slides' list containing enhanced slide data
    """
    
    # Build prompt
    prompt = build_form_enhancement_prompt(service_content)
    
    # Call Gemini
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
    )
    
    # Extract and validate JSON
    data = extract_json(response.text)
    
    # Validation
    if "slides" not in data or not isinstance(data["slides"], list):
        raise ValueError("Invalid slide output from Gemini")
    
    if len(data["slides"]) == 0:
        raise ValueError("No slides generated from form content")
    
    # Re-number slides to ensure sequence
    for i, slide in enumerate(data["slides"], start=1):
        slide["slide_no"] = i
        
        # Validate each slide has required fields
        if not all(k in slide for k in ["title", "bullets", "image_keyword"]):
            raise ValueError(f"Slide {i} missing required fields")
        
        # Ensure bullets is a list
        if not isinstance(slide["bullets"], list):
            raise ValueError(f"Slide {i} bullets must be a list")
    
    return data
# -------------------------------------------------
# TEST
# -------------------------------------------------
# if __name__ == "__main__":
    from utils.pdf_extractor import extract_raw_content

    PDF_PATH = input("Enter PDF path: ").strip()

    if not PDF_PATH:
        print("No PDF path provided, exiting.")
        exit(1)

    RAW_CONTENT = extract_raw_content(PDF_PATH)
    slides = generate_slides_from_raw(RAW_CONTENT)
    print(json.dumps(slides, indent=2))
