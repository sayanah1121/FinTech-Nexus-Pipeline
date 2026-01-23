# Start with the official Airflow image
FROM apache/airflow:2.7.1

# 1. Switch to Root to install system packages (Java)
USER root

# Install OpenJDK-11 (Required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# 2. Switch back to Airflow user to install Python packages
USER airflow

# Copy requirements file from your local folder to the container
COPY requirements.txt /requirements.txt

# Install Python dependencies (PySpark, Delta, Pandas)
RUN pip install --no-cache-dir -r /requirements.txt