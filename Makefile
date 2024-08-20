.PHONY: help license all-requirements requirements requirements-lint requirements-tests benchmark black coverage flake8 integration-test isort lint mypy test unit-test docker-benchmark docker-black docker-coverage docker-flake8 docker-integration-test docker-isort docker-lint docker-mypy docker-test docker-unit-test
SHELL := /bin/bash

help: ## Display this help text
	@grep -E '^[a-zA-Z_-]+[%]?:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

benchmark: PYTEST_ARGS=-m benchmark ## Run benchmarks on the host
benchmark: export PYTEST_ADDOPTS=--benchmark-group-by=group
benchmark: test

unit-test: PYTEST_ARGS=-m unit ## Run unit tests on the host
unit-test: test

integration-test: PYTEST_ARGS=-m integration ## Run integration tests on the host
integration-test: test

test: PYTEST_ARGS_FLAGS=$(if $(PYTEST_ARGS),$(PYTEST_ARGS),-m not benchmark) ## Run unit tests on the host
test:
	PYTEST_ARGS="${PYTEST_ARGS_FLAGS}" tests/scripts/${SCRIPTS_BASE_DIR}run_tests.sh

coverage: export PYTEST_ADDOPTS=--cov=. --cov-context=test --cov-config=.coveragerc --cov-branch ## Run tests with coverage on the host
coverage: export COVERAGE_FILE=.coverage
coverage: test

lint: black flake8 isort mypy  ## Lint the project on the host

black:  ## Run black in the project on the host
	tests/scripts/${SCRIPTS_BASE_DIR}black.sh diff

flake8:  ## Run flake8 in the project on the host
	tests/scripts/${SCRIPTS_BASE_DIR}flake8.sh

isort:  ## Run isort in the project on the host
	tests/scripts/${SCRIPTS_BASE_DIR}isort.sh diff

mypy: ## Run mypy in the project on the host
	tests/scripts/${SCRIPTS_BASE_DIR}mypy.sh

docker-test:  ## Run all tests on docker
docker-test: SCRIPTS_BASE_DIR=docker/
docker-test: test

docker-benchmark:  ## Run benchmarks on docker
docker-benchmark: SCRIPTS_BASE_DIR=docker/
docker-benchmark: benchmark

docker-unit-test:  ## Run unit tests on docker
docker-unit-test: SCRIPTS_BASE_DIR=docker/
docker-unit-test: unit-test

docker-integration-test:  ## Run integration tests on docker
docker-integration-test: SCRIPTS_BASE_DIR=docker/
docker-integration-test: integration-test

docker-coverage:  ## Run tests with coverage on docker
docker-coverage: SCRIPTS_BASE_DIR=docker/
docker-coverage: coverage

docker-lint: docker-black docker-flake8 docker-isort docker-mypy  ## Lint the project on docker

docker-black:  ## Run black in the project on docker
docker-black: SCRIPTS_BASE_DIR=docker/
docker-black: black

docker-flake8:  ## Run flake8 in the project on docker
docker-flake8: SCRIPTS_BASE_DIR=docker/
docker-flake8: flake8

docker-isort:  ## Run isort in the project on docker
docker-isort: SCRIPTS_BASE_DIR=docker/
docker-isort: isort

docker-mypy:  ## Run mypy in the project on docker
docker-mypy: SCRIPTS_BASE_DIR=docker/
docker-mypy: mypy

license:  ## Run license validation in the project
	tests/scripts/license_headers_check.sh check

all-requirements: requirements-lint requirements-tests requirements ## Install all requirements on the host

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
