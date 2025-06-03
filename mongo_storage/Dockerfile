FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY storage_cleaner.py ./

CMD ["python", "storage_cleaner.py"]
