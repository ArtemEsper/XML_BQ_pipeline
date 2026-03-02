# Dataflow Flex Template Dockerfile for WoS XML to BigQuery Pipeline
# Based on Google's official Dataflow template base image

FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# Set working directory
WORKDIR /template

# Set environment variables
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/launcher.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/template/setup.py

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /template/src/
COPY config/ /template/config/
COPY parser/wos_config.xml /template/config/

# Copy metadata
COPY metadata.json /template/

# Copy setup.py, README, and launcher entry point
COPY setup.py /template/
COPY README.md /template/
COPY launcher.py /template/

# Install the package
RUN pip install -e .

# Set permissions
RUN chmod +x /template/src/wos_beam_pipeline/main.py

# The entrypoint is set by the base image
# It will execute FLEX_TEMPLATE_PYTHON_PY_FILE
