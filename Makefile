.PHONY: build run shell test clean

IMAGE_NAME := nfl-dead-money
TAG := latest

build:
	docker build -t $(IMAGE_NAME):$(TAG) .

run:
	docker run --rm -v $(PWD)/reports:/app/reports $(IMAGE_NAME):$(TAG)

shell:
	docker run --rm -it -v $(PWD)/reports:/app/reports $(IMAGE_NAME):$(TAG) /bin/bash

clean:
	docker rmi $(IMAGE_NAME):$(TAG)
