FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
COPY ./server/filter/ .
COPY ./rabbitmq/ ./rabbitmq/

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "main.py"]