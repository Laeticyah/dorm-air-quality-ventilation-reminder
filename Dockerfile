FROM python:3.12-slim

WORKDIR /app
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt
COPY . /app

ENV AIR_DB_PATH=/app/data/air_monitor.db
ENV BACKUP_DIR=/app/data/backups
EXPOSE 8000

CMD ["python", "-m", "uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
