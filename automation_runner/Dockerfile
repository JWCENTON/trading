FROM python:3.11-slim

ARG GIT_SHA
RUN printf '%s' "${GIT_SHA}" | grep -Eq '^([0-9a-f]{40}|[0-9a-f]{64})$'
LABEL org.opencontainers.image.revision="${GIT_SHA}"
ENV GIT_SHA="${GIT_SHA}"

WORKDIR /app

COPY automation_runner/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY common /app/common
COPY scripts /app/scripts
COPY automation_runner/main.py /app/main.py

CMD ["python", "-u", "main.py"]
