"""
PDF Content Extractor with OCR support
Handles both text-based and scanned PDFs
"""

import fitz
import pytesseract
from PIL import Image
import re
import shutil
from config import TESSERACT_CMD, OCR_AVAILABLE, OCR_DPI

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
if TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
else:
    print("⚠️  Tesseract OCR not found. Scanned PDFs will have limited text extraction.")


# -------------------------------------------------
# BASIC CLEAN (LOSSLESS)
# -------------------------------------------------
def clean_line(text):
    """Minimal cleanup - removes extra spaces only"""
    return re.sub(r"[ \t]+", " ", text).strip()


# -------------------------------------------------
# OCR PAGE (RAW)
# -------------------------------------------------
def ocr_page(page):
    """Extract text from page using OCR"""
    if not OCR_AVAILABLE:
        return []

    try:
        pix = page.get_pixmap(dpi=OCR_DPI)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        text = pytesseract.image_to_string(img)
        return [clean_line(l) for l in text.split("\n") if clean_line(l)]
    except Exception as e:
        print(f"⚠️  OCR failed for page: {e}")
        return []


# -------------------------------------------------
# RAW EXTRACTION (TEXT + OCR)
# -------------------------------------------------
def extract_raw_content(pdf_path):
    """
    Extract all text content from PDF

    Returns:
        List of dictionaries with page numbers and lines
    """
    doc = fitz.open(pdf_path)
    pages = []

    for page_no, page in enumerate(doc, start=1):
        page_lines = []

        # Normal text extraction
        text = page.get_text("text")
        for line in text.split("\n"):
            line = clean_line(line)
            if line:
                page_lines.append(line)

        # OCR fallback (ONLY if needed)
        if OCR_AVAILABLE and (len(page_lines) < 10 or page.get_images()):
            ocr_lines = ocr_page(page)

            # Append OCR lines without deduping aggressively
            for l in ocr_lines:
                if l not in page_lines:
                    page_lines.append(l)

        pages.append({"page": page_no, "lines": page_lines})

    return pages


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    """Test the PDF extractor"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python pdf_extractor.py <path_to_pdf>")
        sys.exit(1)

    pdf_path = sys.argv[1]

    print(f"\n📄 Extracting RAW PDF content from: {pdf_path}")
    print(f"🔍 OCR enabled: {OCR_AVAILABLE}\n")

    try:
        pages = extract_raw_content(pdf_path)

        for page in pages:
            print(f"\n{'='*60}")
            print(f"PAGE {page['page']}")
            print("=" * 60)
            for line in page["lines"]:
                print(line)

        print(f"\n✅ Extraction completed: {len(pages)} pages processed")

    except Exception as e:
        print(f"❌ Error extracting PDF: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
