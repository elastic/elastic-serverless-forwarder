.PHONY: help test coverage docker-lint docker-test docker-coverage docker-black docker-flake8 docker-mypy docker-isort black flake8 mypy isort license all-reqs requirements lint-reqs test-reqs
SHELL := /bin/bash

help: ## Display this help text
	@grep -E '^[a-zA-Z_-]+[%]?:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

unit-test: PYTEST_ARGS=-m unit ## Run unit tests on the host
unit-test: test

integration-test: PYTEST_ARGS=-m integration ## Run integration tests on the host
integration-test: test

test:  ## Run all tests on the host
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

docker-unit-test:  ## Run tests on docker
docker-unit-test: PYTEST_ARGS=-m unit
docker-unit-test: docker-test

docker-integration-test:  ## Run tests on docker
docker-integration-test: PYTEST_ARGS=-m integration
docker-integration-test: docker-test


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

all-requirements: requirements requirements-lint requirements-tests  ## Install all requirements on the host

requirements: .makecache/requirements.txt  ## Install app requirements on the host

requirements-lint: .makecache/requirements-lint.txt  ## Install all linting requirements on the host

requirements-tests: .makecache/requirements-tests.txt  ## Install tests requirements on the host

.makecache/requirements.txt: requirements.txt
	pip3 install -r requirements.txt
	touch .makecache/requirements.txt

.makecache/requirements-lint.txt: requirements-lint.txt
	pip3 install -r requirements-lint.txt
	touch .makecache/requirements-lint.txt

.makecache/requirements-tests.txt: requirements-tests.txt
	pip3 install -r requirements-tests.txt
	touch .makecache/requirements-tests.txt
