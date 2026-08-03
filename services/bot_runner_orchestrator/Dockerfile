FROM python:3.11-slim

ARG GIT_SHA
RUN printf '%s' "${GIT_SHA}" | grep -Eq '^([0-9a-f]{40}|[0-9a-f]{64})$'
LABEL org.opencontainers.image.revision="${GIT_SHA}"
ENV GIT_SHA="${GIT_SHA}"

WORKDIR /app
ENV PYTHONPATH=/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
  && rm -rf /var/lib/apt/lists/*

COPY services/bot_runner_orchestrator/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

CMD ["python", "-u", "/app/services/bot_runner_orchestrator/main.py"]
