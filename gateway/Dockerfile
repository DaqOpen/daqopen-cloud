# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/go/dockerfile-reference/

# Want to help us make this template better? Share your feedback here: https://   forms.gle/ybq9Krt8jtBL3iCk7

ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-alpine as base

RUN apk update && apk add tzdata
ENV TZ=Europe/Vienna

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy the source code into the container.
COPY . .

RUN mkdir /app/config

RUN pip install -r requirements.txt

# Run the application.
CMD ["python3", "messagehandler.py"]
