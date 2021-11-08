.PHONY: help
SHELL := /bin/bash

help: ## Display this help text
	@grep -E '^[a-zA-Z_-]+[%]?:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

docker-lint: docker-black docker-flake8 docker-isort docker-mypy ## Lint the project on docker

local-lint: local-black local-flake8 local-isort local-mypy ## Lint the project on docker

docker-test:
	PYTEST_ARGS="${PYTEST_ARGS}" tests/scripts/docker/run_tests.sh

docker-coverage: PYTEST_ARGS=--cov=. --cov-context=test --cov-config=.coveragerc --cov-branch
docker-coverage: export COVERAGE_FILE=.coverage
docker-coverage: docker-test

local-test: requirements tests-reqs
	PYTEST_ARGS="${PYTEST_ARGS}" tests/scripts/run_tests.sh

local-coverage: PYTEST_ARGS=--cov=. --cov-context=test --cov-config=.coveragerc --cov-branch
local-coverage: export COVERAGE_FILE=.coverage
local-coverage: local-test

docker-black:  ## Run black in the project on docker
	tests/scripts/docker/black.sh diff

docker-flake8:  ## Run flake8 in the project on docker
	tests/scripts/docker/flake8.sh

docker-mypy:  ## Run mypy in the project on docker
	tests/scripts/docker/mypy.sh

docker-isort:  ## Run isort in the project on docker
	tests/scripts/docker/isort.sh diff

local-black: requirements lint-reqs  ## Run black in the project on the host
	tests/scripts/black.sh diff

local-flake8: requirements lint-reqs  ## Run flake8 in the project on the host
	tests/scripts/flake8.sh

local-mypy: requirements lint-reqs # # Run mypy in the project on the host
	tests/scripts/mypy.sh

local-isort: requirements lint-reqs  ## Run isort in the project on the host
	tests/scripts/isort.sh diff

license:  ## Run license validation in the project
	tests/scripts/license_headers_check.sh check

all-reqs: requirements lint-reqs tests-reqs

requirements:
	pip3 install -r tests/requirements/requirements.txt

lint-reqs:
	pip3 install -r tests/requirements/lint-black.txt
	pip3 install -r tests/requirements/lint-flake8.txt
	pip3 install -r tests/requirements/lint-isort.txt
	pip3 install -r tests/requirements/lint-mypy.txt

tests-reqs:
	pip3 install -r tests/requirements/tests-reqs.txt
