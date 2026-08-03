FROM python:3.11-slim

ARG GIT_SHA
RUN printf '%s' "${GIT_SHA}" | grep -Eq '^([0-9a-f]{40}|[0-9a-f]{64})$'
LABEL org.opencontainers.image.revision="${GIT_SHA}"
ENV GIT_SHA="${GIT_SHA}"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY bot/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY common /app/common
COPY market_data_worker/main.py /app/main.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "main.py"]
