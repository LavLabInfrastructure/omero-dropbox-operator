# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY src/* /app

# Install any needed packages specified in requirements.txt
COPY requirements.txt /app
RUN pip install --no-cache-dir -r /app/requirements.txt

# Run kopf run on startup
CMD ["kopf", "run", "-m", "/app/OmeroDropboxOperator.py"]
