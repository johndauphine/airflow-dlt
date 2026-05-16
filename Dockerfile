FROM apache/airflow:3.0.0-python3.11

# Microsoft ODBC Driver 18 + Kerberos support for MSSQL via pyodbc.
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         curl \
         gnupg2 \
         unixodbc-dev \
         krb5-user \
         libkrb5-dev \
  && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
