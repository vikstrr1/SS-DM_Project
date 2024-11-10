# Start from the official Apache Flink image
FROM apache/flink:1.17.0

# Set the working directory
WORKDIR /opt/flink/jobs

# Install necessary packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    netcat-openbsd \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies if you have a requirements.txt
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy necessary Python scripts
#COPY stream_emulator.py /opt/flink/jobs/
COPY flink_processor.py /opt/flink/jobs/

#Install Elasticsearch Python Client for saving the data.
RUN pip3 install elasticsearch


# Create data directories for the CSV files
RUN mkdir -p data/trading_data

# Copy utility scripts and make them executable
#COPY wait-for-it.sh .
#COPY retry_stream_emulator.sh .
#RUN chmod +x wait-for-it.sh retry_stream_emulator.sh

# Create a symbolic link for python to point to python3 if it doesn't already exist
RUN [ ! -e /usr/bin/python ] && ln -s /usr/bin/python3 /usr/bin/python || echo "Python link already exists"

# Set the Python executable in Flink configuration
RUN echo 'python.executable: /usr/bin/python3' >> /opt/flink/conf/flink-conf.yaml


# Default entry point with CMD to start appropriate processes
CMD ["./wait-for-it.sh", "kafka:9092", "--", "./retry_stream_emulator.sh"]
