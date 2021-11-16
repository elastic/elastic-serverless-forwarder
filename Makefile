.PHONY: help test coverage docker-lint docker-test docker-coverage docker-black docker-flake8 docker-mypy docker-isort black flake8 mypy isort license all-reqs requirements lint-reqs test-reqs
SHELL := /bin/bash

help: ## Display this help text
	@grep -E '^[a-zA-Z_-]+[%]?:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test:  ## Run tests on the host
	PYTEST_ARGS="${PYTEST_ARGS}" tests/scripts/${BASE_DIR}run_tests.sh

coverage: PYTEST_ARGS=--cov=. --cov-context=test --cov-config=.coveragerc --cov-branch  ## Run tests on the host with coverage
coverage: export COVERAGE_FILE=.coverage
coverage: test

lint: black flake8 isort mypy  ## Lint the project on the host

black:  ## Run black in the project on the host
	tests/scripts/${BASE_DIR}black.sh diff

flake8:  ## Run flake8 in the project on the host
	tests/scripts/${BASE_DIR}flake8.sh

isort:  ## Run isort in the project on the host
	tests/scripts/${BASE_DIR}isort.sh diff

mypy: # # Run mypy in the project on the host
	tests/scripts/${BASE_DIR}mypy.sh

docker-test:  ## Run tests on docker
docker-test: BASE_DIR=docker/
docker-test: test

docker-coverage: PYTEST_ARGS=--cov=. --cov-context=test --cov-config=.coveragerc --cov-branch  ## Run tests on docker with coverage
docker-coverage: export COVERAGE_FILE=.coverage
docker-coverage: docker-test

docker-lint: docker-black docker-flake8 docker-isort docker-mypy  ## Lint the project on docker

docker-black:  ## Run black in the project on docker
docker-black: BASE_DIR=docker/
docker-black: black

docker-flake8:  ## Run flake8 in the project on docker
docker-flake8: BASE_DIR=docker/
docker-flake8: flake8

docker-isort:  ## Run isort in the project on docker
docker-isort: BASE_DIR=docker/
docker-isort: isort

docker-mypy:  ## Run mypy in the project on docker
docker-mypy: BASE_DIR=docker/
docker-mypy: mypy

license:  ## Run license validation in the project
	tests/scripts/license_headers_check.sh check

all-reqs: requirements lint-reqs test-reqs  ## Install all requirements on the host

requirements: .makecache/requirements.txt  ## Install app requirements on the host

lint-reqs: .makecache/lint-black.txt .makecache/lint-flake8.txt .makecache/lint-isort.txt .makecache/lint-mypy.txt  ## Install all linting requirements on the host

test-reqs: .makecache/test-reqs.txt  ## Install tests requirements on the host

.makecache/requirements.txt: requirements.txt
	pip3 install -r requirements.txt
	touch .makecache/requirements.txt

.makecache/lint-black.txt: tests/requirements/lint-black.txt
	pip3 install -r tests/requirements/lint-black.txt
	touch .makecache/lint-black.txt

.makecache/lint-flake8.txt: tests/requirements/lint-flake8.txt
	pip3 install -r tests/requirements/lint-flake8.txt
	touch .makecache/lint-flake8.txt

.makecache/lint-isort.txt: tests/requirements/lint-isort.txt
	pip3 install -r tests/requirements/lint-isort.txt
	touch .makecache/lint-isort.txt

.makecache/lint-mypy.txt: tests/requirements/lint-mypy.txt
	pip3 install -r tests/requirements/lint-mypy.txt
	touch .makecache/lint-mypy.txt

.makecache/test-reqs.txt: tests/requirements/test-reqs.txt
	pip3 install -r tests/requirements/test-reqs.txt
	touch .makecache/test-reqs.txt
