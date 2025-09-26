FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY rabbitmq/ ./rabbitmq/
COPY server/report_generator/ ./
COPY server/config.ini ./config.ini

# Crear directorio para reportes
RUN mkdir -p /app/reports

CMD ["python", "main.py"]
