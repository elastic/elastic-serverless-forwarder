# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import os
from json.decoder import JSONDecodeError
from unittest import TestCase

import pytest
import json

from .parse_notice import NoticeParser


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
                        "path": f"{self.test_license_path}/LICENSE.txt",
                        "licenses": [{"key": "mit"}],
                    }
                ]
            }
        )

        with open(self.scanned_fn, "w+") as fh:
            fh.write(pyyaml_package_data)

        with open(self.test_notice_fn, "w+"):
            pass

        with open(self.test_requirements, "w+") as fh:
            fh.write("PyYAML")

        # os.makedirs(self.test_license_path+"/")

        with open(f"{self.test_license_path}/LICENSE.TXT", "w+") as fh:
            fh.write("THIS IS THE TEST LICENSE FILE CONTENT")

    def tearDown(self) -> None:
        os.remove(self.scanned_fn)
        os.remove(self.test_notice_fn)
        os.remove(self.test_requirements)
        os.rmtree(self.test_license_path)

    def test_init_notice_parser(self) -> None:
        with self.subTest("valid init with nothing to be updated"):
            requirements_files: list[str] = []

            np = NoticeParser(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

            assert np.notice_file_name == self.test_notice_fn
            assert np.requirement_files == requirements_files
            assert np.mode == "check"

        with self.subTest("empty scanned file"):
            with self.assertRaisesRegex(ValueError, f"{self.scanned_fn} is empty"):
                requirements_files = ["requirements.txt", "requirements-lint.txt", "requirements-tests.txt"]
                with open(self.scanned_fn, "w+") as fh:
                    fh.write("")

                NoticeParser(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

            # revert self.scanned_fn to original setup
            self.tearDown()
            self.setUp()

        with self.subTest("scanned_file - not valid json file"):
            with self.assertRaises(JSONDecodeError):
                requirements_files = ["requirements.txt", "requirements-lint.txt", "requirements-tests.txt"]
                with open(self.scanned_fn, "w+") as fh:
                    fh.write("not_a_json")

                NoticeParser(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

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

                NoticeParser(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

            self.tearDown()
            self.setUp()

        with self.subTest("invalid mode type"):
            with self.assertRaisesRegex(SystemExit, "Invalid argument. Please choose a mode between 'fix' or 'check'"):
                requirements_files = ["requirements.txt"]
                NoticeParser(requirements_files, self.scanned_fn, "NOT_A_VALID_MODE", self.test_notice_fn)

    def test_read_requirements(self) -> None:
        with self.subTest("requirements file does not exist"):
            with self.assertRaises(FileNotFoundError):
                requirements_files: list[str] = ["requirements_not_exist.txt"]
                NoticeParser(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

        with self.subTest("succesfully read package from requirements"):
            with open(self.test_requirements, "a") as fh:
                fh.write("\nlocalstack[runtime]")

            requirements_files: list[str] = [self.test_requirements]

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

            np = NoticeParser(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

            assert np.required_packages == {"PyYAML": "PyYAML", "localstack": "localstack[runtime]"}

    def test_check_mode(self) -> None:
        with self.subTest("new packages found"):
            with self.assertRaisesRegex(
                SystemExit, "New packages found. Run the program in 'fix' mode to add it to the NOTICE.txt file"
            ):
                requirements_files = ["requirements.txt", "requirements-lint.txt", "requirements-tests.txt"]
                NoticeParser(requirements_files, self.scanned_fn, "check", self.test_notice_fn)

    def test_read_content_from_file(self) -> None:
        with self.subTest("notice file not found and populate it with the header"):
            requirements_files: list[str] = []

            os.remove(self.test_notice_fn)

            NoticeParser(requirements_files, self.scanned_fn, "check", self.test_notice_fn)
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

                NoticeParser(requirements_files, "FileDoesNotExist", "check", self.test_notice_fn)

        with self.subTest("successfully read content from scanned file"):
            requirements_files: list[str] = []

            with open(self.scanned_fn, "+w") as fh:
                fh.write('{"test_key": "test_value"}')

            np = NoticeParser(requirements_files, self.scanned_fn, "check", self.test_notice_fn)
            assert np.scanned_results_json == {"test_key": "test_value"}

            self.tearDown()
            self.setUp()

    def test_fix_mode(self) -> None:
        with self.subTest("splitted_entry_path in licenses list"):
            requirements_files: list[str] = [self.test_requirements]
            os.remove(self.test_notice_fn)

            np = NoticeParser(requirements_files, self.scanned_fn, "fix", self.test_notice_fn)

            with open(self.test_notice_fn) as fh:
                notice_file_content = fh.read()

            print("NOTICE:", notice_file_content)
            print(np.processed_packages)
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
                + "Time: 2022-01-18 21:07:16\n"
                + "License: MIT\n"
            )
