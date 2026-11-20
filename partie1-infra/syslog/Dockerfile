FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY collecteur_syslog.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "collecteur_syslog.py"]
