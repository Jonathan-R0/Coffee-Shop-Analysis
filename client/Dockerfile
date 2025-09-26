FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
COPY ./client/ .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "main.py"]