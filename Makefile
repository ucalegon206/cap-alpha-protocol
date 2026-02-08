.PHONY: setup shell pipeline test clean

setup:
	docker-compose build

shell:
	docker-compose run --rm pipeline /bin/bash

# Run the full pipeline inside the container
pipeline:
	docker-compose run --rm pipeline python3 run_pipeline.py

# Run tests inside the container
test:
	docker-compose run --rm pipeline pytest tests/

clean:
	docker-compose down -v
