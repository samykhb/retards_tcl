FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

WORKDIR /app

# Installer les dependances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script Spark
COPY spark_batch.py .

# Executer le script de traitement
CMD ["python", "spark_batch.py"]