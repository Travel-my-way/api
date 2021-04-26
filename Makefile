.PHONY: front

image_name ?=localhost:32000
component ?= broker
version ?= latest

front:
	docker-compose up --force-recreate

build:
	docker build . -t ${image_name}/${component}:${version} -f ${component}/Dockerfile

push: build
	docker push ${image_name}/${component}:${version}
