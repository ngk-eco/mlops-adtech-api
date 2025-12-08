FROM python:3.12-slim-bullseye

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Dependencias de sistema para psycopg2
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar la app
COPY main.py .

EXPOSE 8000

# No hardcodees las credenciales acá, las vas a pasar por env en App Runner
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
