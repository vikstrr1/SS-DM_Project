FROM python:3.9-slim
WORKDIR /app

# Install required Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy processor.py into the container
COPY processor.py .

# Run the processor
CMD ["python", "processor.py"]
