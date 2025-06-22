FROM python:3.10-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir flask pika requests

CMD ["python", "manager.py"]
