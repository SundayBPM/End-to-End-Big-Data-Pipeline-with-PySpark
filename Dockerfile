FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["spark-submit", "--jars spark_jars/mysql-connector-j-8.0.33.jar", "app/main.py"]
