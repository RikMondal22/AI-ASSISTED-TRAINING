# utils/pdf_utils.py
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import os
from datetime import datetime


def generate_service_pdf(service_content, output_dir="generated_pdfs"):
    os.makedirs(output_dir, exist_ok=True)

    filename = (
        service_content["service_name"].replace(" ", "_").lower() + "_training.pdf"
    )

    pdf_path = os.path.join(output_dir, filename)

    c = canvas.Canvas(pdf_path, pagesize=A4)
    width, height = A4

    y = height - 40
    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, y, "BSK Training Service Document")

    c.setFont("Helvetica", 10)
    y -= 30
    c.drawString(40, y, f"Generated on: {datetime.now().strftime('%d-%m-%Y %H:%M')}")

    y -= 30

    def write_section(title, text):
        nonlocal y
        c.setFont("Helvetica-Bold", 11)
        c.drawString(40, y, title)
        y -= 18
        c.setFont("Helvetica", 10)
        for line in text.split("\n"):
            if y < 50:
                c.showPage()
                y = height - 40
            c.drawString(50, y, line)
            y -= 14
        y -= 10

    write_section("Service Name", service_content["service_name"])
    write_section("Service Description", service_content["service_description"])
    write_section("How to Apply", service_content["how_to_apply"])
    write_section("Eligibility Criteria", service_content["eligibility_criteria"])
    write_section("Required Documents", service_content["required_docs"])
    write_section("Operator Tips", service_content.get("operator_tips", ""))
    write_section("Troubleshooting", service_content.get("troubleshooting", ""))
    write_section("Fees & Timeline", service_content.get("fees_and_timeline", ""))

    c.save()
    return pdf_path
