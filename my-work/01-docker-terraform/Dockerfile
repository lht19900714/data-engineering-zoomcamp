FROM python:3.9

RUN apt-get install wget

RUN pip install pandas==1.5.3
RUN pip install SQLAlchemy==1.4.51
RUN pip install psycopg2

WORKDIR /app
COPY my_ingest_data.py ingest_data.py
ENTRYPOINT [ "python", "ingest_data.py" ]