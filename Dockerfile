# syntax=docker/dockerfile:1
FROM python:3.10.5-slim-buster
WORKDIR /app
COPY requirements.txt /app/
# RUN pip uninstall spacy
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /app/
# Full 3-pass run via shell script (pass1 → pass2 → item_cache):
# CMD ["/bin/bash", "./run_etl.sh"]
#CMD ["python", "wikidata_dump_etl.py"]
ENTRYPOINT ["python", "wikidata_crawler.py"]
CMD []
