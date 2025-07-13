FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_app.py .
COPY preprocess.py .
COPY config.py .

RUN mkdir -p /app/inputs /app/outputs

ENTRYPOINT ["python", "etl_app.py"]

CMD ["--help"]
