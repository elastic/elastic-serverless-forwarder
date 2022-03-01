# Contributing to the Elastic Serverless Forwarder

If you have a bugfix or new feature that you would like to contribute to
elastic-serverless-forwarder, please find or open an issue about it first. Talk about what you would like to do. It may be that somebody is already working on it, or that there are particular issues that you should know about before implementing the change.

We enjoy working with contributors to get their code accepted. There are many approaches to fixing a problem and it is important to find the best approach before writing too much code.

## Running Elastic Serverless Forwarder locally

We don't provide yet a tool for running Elastic Serverless Forwarder locally. A good first contribution would be to add such support.

## Code structure

The code in the repository is organised according to some conventions.
The folders starting with a dot (`.`) are to be considered internal to Elastic workflow and you should not usually be concerned about them.

In `docs` folder there is the documentation specific to every serverless solution we support (at the moment only AWS Lambda).

The `tests` folder contains both unit and integration tests for the whole code base, structured mimicking the folders/pacakges structure of the main code base. An exception is the `scripts` folder where maintenance helper scripts (usually in `bash`) reside.

We identified so far three components of the project, on top of the serverless function handlers for every cloud solution supported (at the moment only AWS Lambda):
    * `shippers`: the package related to outputs. Either you are sending data to Elasticsearch, Logstash, or anything else, your code must reside here.
    * `storage`: the package related to inputs. Either you are reading data from S3, a bytes blob payload, or anything else, your code must reside here.
    * `share`: the package for common shared utilities that are not related to the above domains and don't contain code related to specific application handling.

In the `handlers` package it resides the code with logic related to the specific serverless solutions, each of them in a specific subpackages (at the moment only AWS Lambda): everything related to a specific cloud serverless solution must reside there.


## Contributing Code Changes

The process for contributing to any of the Elastic repositories is similar.

1. Please make sure you have signed the [Contributor License Agreement](http://www.elastic.co/contributor-agreement/). We are not asking you to assign copyright to us, but to give us the right to distribute your code without restriction. We ask this of all contributors in order to assure our users of the origin and continuing existence of the code. You only need to sign the CLA once.

2. Install the required dependencies. We have three different dependencies sets respectively for the app, linting and tests. You can install them all together or separately, either in a virtualenv or not, according to your preferences. The `make` targets provided are the following:
   * `all-requirements`     Install all requirements on the host
   * `requirements`         Install app requirements on the host
   * `requirements-lint`    Install all linting requirements on the host
   * `requirements-tests`   Install tests requirements on the host

3. Run the linter, license check and test suite to ensure your changes do not break existing code. The `make` targets provided are the following:
   * `lint`                 Lint the project on the host
   * `black`                Run black in the project on the host
   * `isort`                Run isort in the project on the host
   * `mypy`                 Run mypy in the project on the host
   * `license`              Run license validation in the project
   * `test`                 Run all tests on the host
   * `integration-test`     Run integration tests on the host
   * `unit-test`            Run unit tests on the host
   * `coverage`             Run tests on the host with coverage

4. A subset of the previous tasks can be run in docker (that's the method used in CI), these are the equivalent `make` targets provided:
   * `docker-lint`          Lint the project on docker
   * `docker-black`         Run black in the project on docker
   * `docker-isort`         Run isort in the project on docker
   * `docker-mypy`          Run mypy in the project on docker
   * `docker-test`          Run tests on docker
   * `docker-integration-test` Run integration tests on docker
   * `docker-unit-test`     Run unit tests on docker
   * `docker-coverage`      Run tests on docker with coverage

5. Scripts for automated fix of linting and license are provided where available. They are the following:
    * `./tests/scripts/black.sh fix`
    * `./tests/scripts/isort.sh fix`
    * `./tests/scripts/license_headers_check.sh fix`

7. Rebase your changes.   Update your local repository with the most recent code from the main elastic-serverless-forwarder repository, and rebase your branch on top of the latest `main` elastic-serverless-forwarder branch.

8. Submit a pull request. Push your local changes to your forked copy of the repository and submit a pull request. In the pull request, describe what your changes do and mention the number of the issue where discussion has taken place, eg “Closes #123″. Please add or modify tests related to your changes. We tend to reach 100% coverage for all the code outside the `handlers` folder.

Then sit back and wait. There will probably be a discussion about the pull
request and, if any changes are needed, we would love to work with you to get your pull request merged into elastic-serverless-forwarder
