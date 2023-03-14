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
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy && pipenv install gunicorn==20.1.0


FROM python:3.8-alpine AS runtime
COPY --from=python-deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

# Install nginx
# hadolint ignore=DL3018
RUN apk add --no-cache \
            --upgrade \
            --repository http://dl-cdn.alpinelinux.org/alpine/edge/main \
       nginx

COPY ./docker/nginx-api.conf /etc/nginx/nginx.conf

# S6 overlay
ADD https://github.com/just-containers/s6-overlay/releases/download/v1.21.8.0/s6-overlay-amd64.tar.gz /tmp/
# hadolint ignore=DL4006
RUN gunzip -c /tmp/s6-overlay-amd64.tar.gz | tar -xf - -C /

# Services
COPY ./docker/s6/gunicorn /etc/services.d/gunicorn/run
COPY ./docker/s6/nginx-api /etc/services.d/nginx/run

# Create and switch to a new user
RUN addgroup -S appgroup && adduser -S appuser -h /home/appuser -G appgroup
WORKDIR /home/appuser

# Install application into container
COPY ./api /home/appuser/api

EXPOSE 80

ENTRYPOINT ["/init"]
