# ============================================================================
# TrainAI - Dockerfile (venv under /home/ai_bsk)
# ============================================================================

FROM python:3.11-slim

# =============================================================================
# System dependencies
# =============================================================================
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    ffmpeg \
    imagemagick \
    tesseract-ocr \
    tesseract-ocr-eng \
    curl \
    wget \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# =============================================================================
# Fix ImageMagick security policy
# =============================================================================
RUN if [ -f /etc/ImageMagick-7/policy.xml ]; then \
        sed -i 's/rights="none"/rights="read|write"/g' /etc/ImageMagick-7/policy.xml && \
        sed -i '/pattern="@\*"/d' /etc/ImageMagick-7/policy.xml && \
        echo '<policy domain="path" rights="read|write" pattern="@*" />' >> /etc/ImageMagick-7/policy.xml; \
    fi

# =============================================================================
# Create non-root user
# =============================================================================
RUN useradd -m -u 1000 appuser

# =============================================================================
# Set project directory under /home
# =============================================================================
WORKDIR /home/ai_bsk

# =============================================================================
# Create virtualenv INSIDE the project directory
# =============================================================================
ENV VENV_PATH=/home/ai_bsk/venv

RUN python -m venv ${VENV_PATH}

# Always use the venv
ENV PATH="${VENV_PATH}/bin:$PATH"

# =============================================================================
# Install Python dependencies into the venv
# =============================================================================
COPY Requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r Requirements.txt

# =============================================================================
# Copy application source
# =============================================================================
COPY . .

# =============================================================================
# Create required directories & permissions
# =============================================================================
RUN mkdir -p \
    backend/images backend/videos backend/output_videos \
    backend/uploads backend/generated_pdfs backend/temp \
    assets/avatar \
    && chown -R appuser:appuser /home/ai_bsk

# Switch to non-root user
USER appuser

# =============================================================================
# Environment variables
# =============================================================================
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/home/ai_bsk \
    TESSERACT_CMD=/usr/bin/tesseract \
    IMAGEMAGICK_BINARY=/usr/bin/convert \
    PORT=54300

# =============================================================================
# Volumes
# =============================================================================
VOLUME ["/home/ai_bsk/backend/videos", \
        "/home/ai_bsk/backend/output_videos", \
        "/home/ai_bsk/backend/images", \
        "/home/ai_bsk/backend/uploads", \
        "/home/ai_bsk/backend/generated_pdfs"]

# =============================================================================
# Healthcheck
# =============================================================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# =============================================================================
# Expose & run
# =============================================================================
EXPOSE 54300

CMD ["python", "backend/run.py"]
