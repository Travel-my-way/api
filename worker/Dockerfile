FROM python:3.8-slim-buster as base

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1


FROM base AS python-deps
# Install pipenv and compilation dependencies
RUN pip install --no-cache-dir pipenv=='2023.2.18' && apt-get update && apt-get install -y --no-install-recommends gcc=4:8.3.0-1

# Install python dependencies in /.venv
COPY Pipfile .
COPY Pipfile.lock .
RUN PIPENV_VENV_IN_PROJECT=1 pipenv --bare install --deploy


FROM base AS runtime
COPY --from=python-deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

# S6 overlay
ADD https://github.com/just-containers/s6-overlay/releases/download/v1.21.8.0/s6-overlay-amd64.tar.gz /tmp/
# hadolint ignore=DL4006
RUN gunzip -c /tmp/s6-overlay-amd64.tar.gz | tar -xf - -C /

# Create and switch to a new user
RUN addgroup --gid 2000 appgroup && adduser --home /home/appuser --gid 2000 appuser
WORKDIR /home/appuser

# Install application & startup script into container
COPY ./docker/s6/celery-worker /etc/services.d/worker/run
COPY ./worker /home/appuser/worker

ENTRYPOINT ["/init"]
