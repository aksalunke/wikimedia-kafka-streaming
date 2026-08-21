FROM python:3.13-slim

WORKDIR /app

COPY consumer/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer/consumer.py consumer/consumer.py

CMD ["python", "consumer/consumer.py"]