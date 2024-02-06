# Use an official Python runtime as a parent image
FROM python:3.9-slim-bookworm
RUN apt-get update && apt-get install -y inotify-tools && apt-get clean
# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY src/* /app

# Install any needed packages specified in requirements.txt
COPY requirements.txt /app
RUN pip install --no-cache-dir -r /app/requirements.txt

# Run kopf run on startup
ENTRYPOINT /app/entrypoint.sh
