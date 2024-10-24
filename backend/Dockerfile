FROM python:3.9

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code only
COPY app/ ./app/

# Use a lightweight base image if you're using Flink
# Uncomment the line below if you want to install Flink separately in your image
# RUN apt-get update && apt-get install -y flink
ENV PYTHONPATH=/app

CMD ["python", "app/query_handler.py"]
