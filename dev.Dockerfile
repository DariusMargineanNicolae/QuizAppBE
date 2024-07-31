FROM python:3.10-slim

WORKDIR /app

RUN apt update
RUN apt install  -y

RUN apt-get update \
&& apt-get install ffmpeg libsm6 libxext6 build-essential git ssh gcc curl unzip -y \
&& apt-get clean

RUN pip install --upgrade pip
# RUN pip install poetry
RUN pip install aws-sam-cli

# RUN curl -sSL https://sdk.cloud.google.com | bash
# ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install

RUN python3 -m venv /opt/venv
COPY . .

EXPOSE 8000
