FROM python:3.10-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY kafka/producer.py .
COPY scripts/utils.py .
CMD ["python", "producer.py"]