FROM rabbitmq-python-base:0.0.1

WORKDIR /app

COPY requirements.txt .
COPY ./client/ .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "main.py"]