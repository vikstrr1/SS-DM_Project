# Use OpenJDK 11 for compatibility with Flink
FROM openjdk:11-jdk-slim

# Set the working directory
WORKDIR /opt/flink/jobs

# Install Python, Pip, and netcat for Kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Python3 as the default version for 'python'
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Update PATH environment variable for compatibility
ENV PATH="/usr/local/bin/python3:${PATH}"

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy necessary Python scripts
COPY stream_emulator.py .
COPY flink_processor.py .

# Create data directories for the CSV files
RUN mkdir -p data/trading_data

# Copy CSV file into the container
COPY data/trading_data/debs2022-gc-trading-day-14-11-21.csv ./data/trading_data/

# Copy utility scripts and make them executable
COPY wait-for-it.sh .
COPY retry_stream_emulator.sh .
RUN chmod +x wait-for-it.sh retry_stream_emulator.sh

# Default entry point with CMD to start appropriate processes
# Use 'CMD' to allow dynamic run options (default: stream emulator)
CMD ["./wait-for-it.sh", "kafka:9092", "--", "./retry_stream_emulator.sh"]
