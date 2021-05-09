FROM python:3.8-slim as base

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1


FROM base AS python-deps
# Install pipenv and compilation dependencies
RUN pip install --no-cache-dir pipenv=='2020.11.15' && apt-get update && apt-get install -y --no-install-recommends gcc=4:8.3.0-1

# Install python dependencies in /.venv
COPY Pipfile .
COPY Pipfile.lock .
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy


FROM python:3.8-alpine AS runtime
COPY --from=python-deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

# Create and switch to a new user
RUN addgroup -S appgroup && adduser -S appuser -h /home/appuser -G appgroup
WORKDIR /home/appuser
USER appuser

# Install application into container
COPY ./api /app/api

ENTRYPOINT ["flask", "run", "--host=0.0.0.0"]
