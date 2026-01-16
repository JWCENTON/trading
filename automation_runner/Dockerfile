FROM python:3.11-slim

WORKDIR /app

COPY automation_runner/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY common /app/common
COPY scripts /app/scripts
COPY automation_runner/main.py /app/main.py

CMD ["python", "-u", "main.py"]
