# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import json
import os
from datetime import datetime
from json.decoder import JSONDecodeError
from unittest import TestCase

import mock
import pytest
from mock import MagicMock

from .notice_generator import NoticeGenerator


def mock_license_content(_: str) -> MagicMock:
    response = MagicMock()
    response.text = "THIS IS THE TEST LICENSE FILE CONTENT"
    response.status_code = 200
    return response


@pytest.mark.unit
class TestParseNotice(TestCase):
    def setUp(self) -> None:
        self.scanned_fn = "TEST_SCANNED.json"
        self.test_notice_fn = "TEST_NOTICE.txt"
        self.test_requirements = "TEST_REQUIREMENTS.txt"
        self.test_license_path = "THIS/IS/A/TEST/PyYAML-5.4.1.dist-info"

        pyyaml_package_data = json.dumps(
            {
                "files": [
                    {
                        "path": f"{self.test_license_path}/METADATA",
                        "licenses": [
                            {
                                "key": "mit",
                                "scancode_text_url": "https://github.com/nexB/scancode-toolkit/tree/develop/src/"
                                "licensedcode/data/licenses/mit.LICENSE",
                            },
                        ],
                        "packages": [{"homepage_url": "https://pyyaml.org/"}],
                    },
                ]
            }
        )

        with open(self.scanned_fn, "w+") as fh:
            fh.write(pyyaml_package_data)

        with open(self.test_notice_fn, "w+"):
            pass

        with open(self.test_requirements, "w+") as fh:
            fh.write("PyYAML")

        if not os.path.exists(self.test_license_path):
            os.makedirs(self.test_license_path + "/")

        with open(f"{self.test_license_path}/LICENSE", "w+") as fh:
            fh.write("THIS IS THE TEST LICENSE FILE CONTENT")

    def tearDown(self) -> None:
        os.remove(self.scanned_fn)
        os.remove(self.test_notice_fn)
        os.remove(self.test_requirements)
        os.remove(f"{self.test_license_path}/LICENSE")
        os.removedirs(self.test_license_path)

    def test_init_notice_parser(self) -> None:
        with self.subTest("valid init with nothing to be updated"):
            requirements_files: list[str] = []

            np = NoticeGenerator(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

            assert np.notice_file_name == self.test_notice_fn
            assert np.requirement_files == requirements_files
            assert np.mode == "check"

        with self.subTest("empty scanned file"):
            with self.assertRaises(JSONDecodeError):
                requirements_files = ["requirements.txt", "requirements-lint.txt", "requirements-tests.txt"]
                with open(self.scanned_fn, "w+") as fh:
                    fh.write("")

                NoticeGenerator(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

            # revert self.scanned_fn to original setup
            self.tearDown()
            self.setUp()

        with self.subTest("scanned_file - not valid json file"):
            with self.assertRaises(JSONDecodeError):
                requirements_files = ["requirements.txt", "requirements-lint.txt", "requirements-tests.txt"]
                with open(self.scanned_fn, "w+") as fh:
                    fh.write("not_a_json")

                NoticeGenerator(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

            self.tearDown()
            self.setUp()

        with self.subTest("package in notice file but not in requirements"):
            with self.assertRaisesRegex(
                SystemExit, "Package 'PyYAML' exists in TEST_NOTICE.txt, but not in requirements"
            ):
                requirements_files = []

                with open(self.test_notice_fn, "a") as fh:
                    fh.write("\n\n" + "-" * 100 + "\n")
                    fh.write("Package: PyYAML\n")
                    fh.write("Version: 5.4.1\n")
                    fh.write("Homepage: https://pyyaml.org/\n")
                    fh.write("Time: 2022-01-18 21:07:16\n")
                    fh.write("License: MIT\n")

                NoticeGenerator(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

            self.tearDown()
            self.setUp()

        with self.subTest("invalid mode type"):
            with self.assertRaisesRegex(SystemExit, "Invalid argument. Please choose a mode between 'fix' or 'check'"):
                requirements_files = ["requirements.txt"]
                NoticeGenerator(requirements_files, self.scanned_fn, "NOT_A_VALID_MODE", self.test_notice_fn)

    def test_read_requirements(self) -> None:
        with self.subTest("requirements file does not exist"):
            with self.assertRaises(FileNotFoundError):
                requirements_files: list[str] = ["requirements_not_exist.txt"]
                NoticeGenerator(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

        with self.subTest("succesfully read package from requirements"):
            with open(self.test_requirements, "a") as fh:
                fh.write("\nlocalstack[runtime]")

            requirements_files = [self.test_requirements]

            with open(self.test_notice_fn, "a") as fh:
                fh.write("\n\n" + "-" * 100 + "\n")
                fh.write("Package: PyYAML\n")
                fh.write("Version: 5.4.1\n")
                fh.write("Homepage: https://pyyaml.org/\n")
                fh.write("Time: 2022-01-18 21:07:16\n")
                fh.write("License: MIT\n")
                fh.write("\n\n" + "-" * 100 + "\n")
                fh.write("Package: localstack[runtime]\n")
                fh.write("Version: 0.12.20\n")
                fh.write("Homepage: https://github.com/localstack/localstack\n")
                fh.write("Time: 2022-01-18 21:07:16\n")
                fh.write("License: APACHE-2.0\n")

            np = NoticeGenerator(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

            assert np.required_packages == {"PyYAML": "PyYAML", "localstack": "localstack[runtime]"}

    def test_check_mode(self) -> None:
        with self.subTest("new packages found"):
            with self.assertRaisesRegex(
                SystemExit, "New packages found. Run the program in 'fix' mode to add it to the TEST_NOTICE.txt file"
            ):
                requirements_files = ["requirements.txt", "requirements-lint.txt", "requirements-tests.txt"]
                NoticeGenerator(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

    def test_read_content_from_file(self) -> None:
        with self.subTest("notice file not found and populate it with the header"):
            requirements_files: list[str] = []

            os.remove(self.test_notice_fn)

            NoticeGenerator(requirements_files, self.scanned_fn, "check", self.test_notice_fn)
            with open(self.test_notice_fn) as fh:
                notice_file_content = fh.read()

            assert (
                notice_file_content
                == "# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one\n"
                + "# or more contributor license agreements. Licensed under the Elastic License 2.0;\n"
                + "# you may not use this file except in compliance with the Elastic License 2.0.\n"
                + "\n"
                + "Elastic Serverless Forwarder\n"
                + 100 * "="
                + "\n"
                + "Third party libraries used by the Elastic Serverless Forwarder project:\n"
                + 100 * "="
            )

        with self.subTest("scanned file does not exist"):
            with self.assertRaises(FileNotFoundError):
                requirements_files = []

                NoticeGenerator(requirements_files, "FileDoesNotExist", "check", self.test_notice_fn)

        with self.subTest("successfully read content from scanned file"):
            requirements_files = []

            with open(self.scanned_fn, "+w") as fh:
                fh.write('{"test_key": "test_value"}')

            np = NoticeGenerator(requirements_files, self.scanned_fn, "check", self.test_notice_fn)
            assert np.scanned_results_json == {"test_key": "test_value"}

            self.tearDown()
            self.setUp()

    @mock.patch("tests.scripts.notice_generator.requests.get", new=mock_license_content)
    def test_fix_mode(self) -> None:
        with self.subTest("successfully write to notice file"):
            requirements_files: list[str] = [self.test_requirements]
            os.remove(self.test_notice_fn)

            np = NoticeGenerator(requirements_files, self.scanned_fn, "fix", self.test_notice_fn)

            with open(self.test_notice_fn) as fh:
                notice_file_content = fh.read()

            assert np.processed_packages["PyYAML"]["package_name"] == "PyYAML"
            assert np.processed_packages["PyYAML"]["license_name"] == "mit"
            assert np.processed_packages["PyYAML"]["license_content"] == "THIS IS THE TEST LICENSE FILE CONTENT"
            assert np.processed_packages["PyYAML"]["version"] == "5.4.1"
            assert np.processed_packages["PyYAML"]["homepage_url"] == "https://pyyaml.org/"
            assert (
                notice_file_content
                == "# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one\n"
                + "# or more contributor license agreements. Licensed under the Elastic License 2.0;\n"
                + "# you may not use this file except in compliance with the Elastic License 2.0.\n"
                + "\n"
                + "Elastic Serverless Forwarder\n"
                + 100 * "="
                + "\n"
                + "Third party libraries used by the Elastic Serverless Forwarder project:\n"
                + 100 * "="
                + "\n\n"
                + "-" * 100
                + "\n"
                + "Package: PyYAML\n"
                + "Version: 5.4.1\n"
                + "Homepage: https://pyyaml.org/\n"
                + f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\n"
                + "License: mit\n\n\n"
                + "Contents of the licence https://github.com/nexB/scancode-toolkit/tree/develop/src/licensedcode/data/"
                "licenses/mit.LICENSE: \n\n" + "THIS IS THE TEST LICENSE FILE CONTENT"
            )