FROM python:3.8-slim

WORKDIR /app
COPY Pipfile* /app/
RUN pip install pipenv==2020.8.13 && pipenv install --system --deploy

COPY ./broker /app/broker

ENTRYPOINT ["python", "-m", "broker"]
