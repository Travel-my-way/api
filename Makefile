.PHONY: front
VERSION = $(shell git describe --tags --abbrev=0)
NEXT_VERSION = $(shell scripts/semver bump ${step} ${VERSION})

image_name ?=localhost:32000
component ?= broker
version ?= latest
step ?= "minor"

front:
	docker-compose up --force-recreate

build:
	docker build . -t ${image_name}/${component}:${version} -f ${component}/Dockerfile

push: build
	docker push ${image_name}/${component}:${version}


all:
	@echo "Please read this file !"

is_dirty:
	@git diff --cached --quiet --exit-code || (echo "Master in dirty state !!" && exit 1)

tag: is_dirty
	@echo "Current version: ${VERSION}"
	@echo "Next version: ${NEXT_VERSION}"
	git tag -a v${NEXT_VERSION} -m '${step} bump to v${NEXT_VERSION}'

serve-docs:
	docsify serve ./docs
