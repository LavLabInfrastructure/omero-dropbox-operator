FROM python:3.9-slim-bookworm

RUN apt-get update && apt-get install -y inotify-tools && apt-get clean

WORKDIR /app

COPY requirements.txt /app
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/* /app
RUN chmod +x /app/entrypoint.sh

USER 1000
ENTRYPOINT /app/entrypoint.sh
