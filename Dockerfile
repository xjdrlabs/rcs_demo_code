FROM python:3.7-buster

RUN pip3 install pipenv

WORKDIR /src

ADD . /src

RUN pipenv install --dev

ENV GOOGLE_APPLICATION_CREDENTIALS=/var/secret/credentials.json

CMD ["./bin/run.sh"]
