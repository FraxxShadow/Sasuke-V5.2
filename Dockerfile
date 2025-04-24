# Use Python 3.12 slim as the base image
FROM python:3.12-slim

# Install system dependencies, including ffmpeg
RUN apt update && apt install -y \
    ffmpeg \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*  # Clean up to reduce image size

# Set the working directory in the container
WORKDIR /app

# Copy your application code to the container
COPY . /app/

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Install Python dependencies from requirements.txt
RUN pip install -r requirements.txt

# Define the command to run your bot
CMD ["python", "bot.py"]
