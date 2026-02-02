FROM apache/airflow:2.7.1

# 1. Switch to Root to install system packages
USER root

# Install OpenJDK-11 (Required for PySpark) & procps (for system utilities)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# 2. Switch back to Airflow user to install Python packages
USER airflow

# Copy requirements file from your local folder to the container
COPY requirements.txt /requirements.txt

# Install Python dependencies (PySpark, Delta, Pandas, etc.)
RUN pip install --no-cache-dir -r /requirements.txt