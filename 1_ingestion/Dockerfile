# To build the producer container

FROM python:3.11-slim

WORKDIR /app

# Copy dependecies and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy python scripts
COPY producer_tcl.py .
COPY consumer_test.py .

CMD ["python", "-u", "producer_tcl.py"]