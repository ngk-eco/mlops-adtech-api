FROM python:3.12-slim-bullseye

# Evitar prompts interactivos de apt
ENV DEBIAN_FRONTEND=noninteractive

# Crear directorio de trabajo
WORKDIR /app

# Instalar dependencias de sistema mínimas (útil para psycopg2-binary)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalarlos
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar el código de la app
COPY main.py .

# Puerto interno de la app (App Runner lo mapea hacia afuera)
EXPOSE 8000

# Variables de entorno opcionales para la DB (se pueden overridear en App Runner)
ENV DB_HOST=mlops-tp-db.cf4i6e6cwv74.us-east-1.rds.amazonaws.com \
    DB_PORT=5432 \
    DB_NAME=postgres \
    DB_USER=grupo_6_2025 \
    DB_PASSWORD=NubeMLOPS!

# Comando para levantar FastAPI con Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
