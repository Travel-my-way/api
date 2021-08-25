FROM python:3.8-slim-buster as base

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


FROM python:3.8-slim-buster AS runtime
COPY --from=python-deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

# S6 overlay
ADD https://github.com/just-containers/s6-overlay/releases/download/v1.21.8.0/s6-overlay-amd64.tar.gz /tmp/
# hadolint ignore=DL4006
RUN gunzip -c /tmp/s6-overlay-amd64.tar.gz | tar -xf - -C /

# Reinstall numpy
RUN python -m pip install numpy==1.20.3


# Create and switch to a new user
RUN groupadd -r appgroup -g 901 && useradd -m -g appgroup -u 901 appuser
WORKDIR /home/appuser

# Install application & startup script into container
COPY ./docker/s6/celery-broker /etc/services.d/worker/run
COPY ./worker /home/appuser/worker
COPY ./broker /home/appuser/broker

ENTRYPOINT ["/init"]
