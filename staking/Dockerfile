FROM python:3.10.4-slim

COPY . /code
WORKDIR /code

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
  && apt-get -y install openjdk-11-jdk git g++ \
  && apt-get clean

# install python dependencies
RUN pip install --upgrade pip
RUN pip install -r ./staking/requirements.txt

ENTRYPOINT python -m staking