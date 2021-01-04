FROM python:3.8-slim

WORKDIR /app
COPY Pipfile* /app/
RUN pip install pipenv==2020.8.13 && pipenv install --system --deploy

COPY ./api /app/api

ENTRYPOINT ["flask", "run", "--host=0.0.0.0"]
