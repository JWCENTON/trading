FROM python:3.11-slim

WORKDIR /app

# requirements są w podkatalogu
COPY regime_worker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# kopiujemy kod
COPY common /app/common
COPY regime_worker/main.py /app/main.py

CMD ["python", "-u", "main.py"]