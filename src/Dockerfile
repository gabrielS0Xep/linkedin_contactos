FROM python:3.9-slim

# Configurar directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema si es necesario
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivo de requerimientos primero (para aprovechar cache de Docker)
COPY requirements.txt ./

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r ./requirements.txt

# Copiar el código de la aplicación
COPY src/ ./src

# Work from src so imports like `from config import Config` resolve
WORKDIR /app/src

# Crear usuario no-root por seguridad
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app
USER app

# Configurar variables de entorno por defecto
ENV FLASK_HOST=0.0.0.0
ENV PORT=8080
ENV FLASK_DEBUG=False
ENV MAX_WORKERS=3
ENV REQUEST_TIMEOUT=300

# Exponer el puerto
EXPOSE 8080

# Configurar health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Comando para ejecutar la aplicación
CMD ["python", "main.py"] 