FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /opt/app

# Upgrade pip
RUN pip install --upgrade pip

COPY requirements.txt .

RUN pip install -r requirements.txt

WORKDIR /opt/app/dagster
ENV DAGSTER_HOME /opt/app/dagster

COPY workspace.yaml load_forecasting ./

EXPOSE 3000

ENTRYPOINT [ "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]