# Using requirements.txt
FROM apache/airflow:2.8.0

ARG DWH_ENV
ENV DWH_ENV $DWH_ENV

# Upgrade pip
RUN python -m pip install --upgrade pip

# Set the working directory
WORKDIR /opt/airflow

# Copy your airflow.cfg into the image
COPY airflow.cfg /opt/airflow/airflow.cfg

# Copy the requirements.txt file
COPY requirements.txt .

# Install the dependencies from requirements.txt
RUN python -m pip install --user --no-cache-dir -r requirements.txt
