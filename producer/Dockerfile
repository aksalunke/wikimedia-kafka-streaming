FROM python:3.13-slim

WORKDIR /app

COPY producer/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/producer.py producer/producer.py

CMD ["python", "producer/producer.py"]