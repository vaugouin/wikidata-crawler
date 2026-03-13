# syntax=docker/dockerfile:1
FROM python:3.10.5-slim-buster
WORKDIR /app
COPY requirements.txt /app/
# RUN pip uninstall spacy
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /app/
CMD ["/bin/bash", "./run_etl.sh"]
