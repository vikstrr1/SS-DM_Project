## Use the official Python image
FROM python:3.11-slim

# Set the working directory
WORKDIR /opt/flink

# Copy the requirements file first for caching
COPY ./requirements.txt .

# Install PyFlink and dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the job and data files into the container
COPY ./flink /opt/flink/jobs
COPY ./data /opt/flink/jobs/data

# Set the default command to run your job
CMD ["python", "/opt/flink/jobs/job.py"]
