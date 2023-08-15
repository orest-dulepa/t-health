FROM python:3.9.5-slim

WORKDIR /usr/src/app
COPY requirements*.txt ./

RUN apt-get update -qq \
    && apt-get install --no-install-recommends -qqy gcc libpq-dev python3-dev \
    && apt-get clean \
    && pip install --quiet --upgrade pip \
    && pip install --quiet --trusted-host pypi.python.org --requirement requirements-dev.txt
