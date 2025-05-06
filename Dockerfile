# Use the official Airflow image as base
FROM apache/airflow:2.8.1-python3.9

# Switch to root user temporarily to install system packages
USER root

# Update package list and install OpenJDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Define the JAVA_HOME environment variable for OpenJDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Add the Java bin directory to the PATH
ENV PATH=$PATH:$JAVA_HOME/bin

# Switch back to the default Airflow user
USER airflow

# Copy the requirements file and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt