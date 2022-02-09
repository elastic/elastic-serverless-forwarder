# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import argparse
import json
import re
from datetime import datetime
from typing import Any

import requests
from requests import Response


class NoticeParser:
    POSSIBLE_METADATA_FILES: list[str] = ["METADATA", "METADATA.txt"]

    def __init__(
        self, requirement_files: list[str], scanned_json_file: str, cli_mode_argument: str, notice_fn: str
    ) -> None:
        self.requirement_files: list[str] = requirement_files
        self.scanned_json_file: str = scanned_json_file
        self.processed_packages: dict[str, dict[str, str]] = {}
        self.required_packages: dict[str, str] = {}
        self.notice_file_name: str = notice_fn
        self.mode: str = cli_mode_argument

        self.read_requirements()

        scanned_results_data = self.read_content_from_file(self.scanned_json_file)

        if not scanned_results_data:
            raise ValueError(f"{self.scanned_json_file} is empty")

        try:
            self.scanned_results_json: Any = json.loads(scanned_results_data)
        except Exception as e:
            raise e

        notice_file_content: str = self.read_content_from_file(self.notice_file_name)

        package_pattern = r"(?:Package: ([^\n]+))"
        existing_packages: list[str] = re.findall(package_pattern, notice_file_content)
        existing_packages.sort()

        requirements_name_from_file = [requirement for requirement in self.required_packages.keys()]
        requirements_name_from_file.sort()

        real_requirements_name = [requirement for requirement in self.required_packages.values()]
        real_requirements_name.sort()

        if real_requirements_name == existing_packages:
            print("There is no new package listed in the requirements files")
            return

        for package in existing_packages:
            if package not in self.required_packages.values():
                raise SystemExit(f"Package '{package}' exists in {self.notice_file_name}, but not in requirements")

        if self.mode == "check":
            for new_package in requirements_name_from_file:
                if self.required_packages[new_package] not in existing_packages:
                    real_package_name: str = self.required_packages[new_package]
                    print(f"New package found: '{real_package_name}'")

            raise SystemExit(
                f"New packages found. Run the program in 'fix' mode to add it to the {notice_file_name} file"
            )

        elif self.mode == "fix":
            for new_package in requirements_name_from_file:
                if self.required_packages[new_package] not in existing_packages:
                    real_package_name = self.required_packages[new_package]

                    print(f"New package found: '{real_package_name}'")
                    self.process_package(required_package=new_package)
                    self.verify_license_in_packages(processed_package=new_package)
                    processed_package = self.processed_packages.get(new_package)

                    if not processed_package:
                        print(
                            f"Missing data for '{real_package_name}' in {self.scanned_json_file}",
                        )
                        continue

                    if "package_name" not in processed_package or "license_name" not in processed_package:
                        print(f"Missing data for '{real_package_name}'. Skipping...")
                        continue

                    self.write_to_notice_file(processed_package)
                    print(f"Package '{real_package_name}' has been added to {self.notice_file_name}")
        else:
            raise SystemExit("Invalid argument. Please choose a mode between 'fix' or 'check'")

    def process_package(self, required_package: str) -> None:
        """
        Iterates over the json file outputted by scancode and tries to find a match between
        the required package and the installed one and looks in the self.POSSIBLE_LICENSE_FILES
        and self.POSSIBLE_METADATA_FILES where the important information about the package should exist
        """
        for entry in self.scanned_results_json["files"]:
            # eg. entry["path"] = venv/lib/python3.9/site-packages/package_name-2.1.3.dist-info/METADATA
            splitted_entry_path: list[str] = entry["path"].split("/")

            if splitted_entry_path[-1] in NoticeParser.POSSIBLE_METADATA_FILES:
                package_name_and_version: list[str] = []

                for i in range(len(splitted_entry_path)):
                    if required_package in splitted_entry_path[i]:
                        package_name_and_version = splitted_entry_path[i].rstrip(".dist-info").split("-")

                if not package_name_and_version:
                    continue

                package_name = package_name_and_version[0]

                if len(package_name_and_version) > 1:
                    package_version = package_name_and_version[1]
                else:
                    package_version = ""

                if package_name == required_package:
                    if package_name not in self.processed_packages:
                        self.processed_packages[package_name] = {}
                        self.processed_packages[package_name]["package_name"] = self.required_packages[required_package]
                        self.processed_packages[package_name]["version"] = package_version

                    if splitted_entry_path[-1] in NoticeParser.POSSIBLE_METADATA_FILES:
                        self.processed_packages[package_name]["license_name"] = entry["licenses"][0]["key"]

                        homepage_url: str = entry["packages"][0]["homepage_url"]
                        self.processed_packages[package_name]["homepage_url"] = homepage_url

                        scancode_url: str = entry["licenses"][0].get("scancode_text_url")
                        self.processed_packages[package_name]["scancode_url"] = scancode_url

    def read_content_from_file(self, content_file_path: str) -> str:
        """
        Reads a file and returns the string representation of its content
        """
        try:
            with open(content_file_path) as fh:
                file_content: str = fh.read()

        except FileNotFoundError as fnf:
            if content_file_path == self.notice_file_name:
                with open(self.notice_file_name, "w+") as fh:
                    fh.write("# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one\n")
                    fh.write("# or more contributor license agreements. Licensed under the Elastic License 2.0;\n")
                    fh.write("# you may not use this file except in compliance with the Elastic License 2.0.\n\n")
                    fh.write("Elastic Serverless Forwarder\n")
                    fh.write("=" * 100)
                    fh.write("\n")
                    fh.write("Third party libraries used by the Elastic Serverless Forwarder project:\n")
                    fh.write("=" * 100)

                with open(self.notice_file_name) as fh:
                    file_content = fh.read()

                return file_content
            else:
                raise fnf

        except Exception as e:
            raise e
        else:
            return file_content

    def read_requirements(self) -> None:
        """
        Reads the inputted requirements and creates the self.required_pacakges dict that contains
        parsed package name as key and original package name as value (eg. "elastic_apm": "elastic-apm")

        We need the parsed version of the package because the folder name where the package exists
        has "_" instead "-" (eg. "venv/.../elastic_apm-6.7.2.dist-info/...)
        """
        for requirement_file in self.requirement_files:
            try:
                with open(requirement_file) as fh:
                    req_data: list[str] = fh.readlines()

            except FileNotFoundError as fnf:
                raise fnf
            except Exception as e:
                raise e
            else:
                for original_requirement in req_data:
                    cleaned_requirement_name: str = original_requirement.split("=")[0].strip(">").strip("\n")
                    package_name: str = cleaned_requirement_name.replace("-", "_")

                    if package_name not in self.required_packages:
                        if "[" and "]" in package_name:
                            package_name = package_name.split("[")[0]

                        self.required_packages[package_name] = cleaned_requirement_name

    def verify_license_in_packages(self, processed_package: str) -> None:
        """
        Checks if the license_content exists for all packages
        If license not found, it builds the url for the raw github text and fetches the license content
        """
        if (
            processed_package in self.processed_packages
            and "license_content" not in self.processed_packages[processed_package]
        ):
            try:
                raw_github_base_url: str = "https://raw.githubusercontent.com"
                scancode_url: str = self.processed_packages[processed_package]["scancode_url"]
                scancode_license_content_url: str = ""

                if scancode_url:
                    splitted_scancode_url: list[str] = scancode_url.split("/")

                    if "tree" in splitted_scancode_url:
                        splitted_scancode_url.remove("tree")

                    scancode_license_content_url = f"{raw_github_base_url}/{'/'.join(splitted_scancode_url[3:])}"

                response: Response = requests.get(scancode_license_content_url)

                if response.status_code == 200:
                    self.processed_packages[processed_package]["license_content"] = response.text
                    self.processed_packages[processed_package]["license_path"] = scancode_url
                else:
                    print(f"License could not be found at: {scancode_url}")

            except Exception as e:
                raise e

    def write_to_notice_file(self, package_data: dict[str, str]) -> None:
        """
        Writes the NOTICE.txt file with the package data
        """
        package_name = package_data["package_name"]
        package_version = package_data.get("version")
        package_homepage_url = package_data.get("homepage_url")
        package_license_name = package_data["license_name"]
        package_license_path = package_data.get("license_path")
        package_license_content = package_data.get("license_content")

        if not package_version:
            package_version = ""

        if not package_homepage_url:
            package_homepage_url = ""

        if not package_license_path:
            package_license_path = ""

        if not package_license_content:
            package_license_content = ""

        with open(self.notice_file_name, "a+") as fh:
            fh.write("\n\n")
            fh.write("-" * 100)
            fh.write("\n")
            fh.write(f"Package: {package_name}\n")
            fh.write(f"Version: {package_version}\n")
            fh.write(f"Homepage: {package_homepage_url}\n")
            fh.write(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\n")
            fh.write(f"License: {package_license_name}\n")
            fh.write("\n\n")
            fh.write(f"Contents of the licence {package_license_path}: \n\n")
            fh.write(package_license_content)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check for packages' licenses listed in requirements files and add them to NOTICE.txt"
    )
    parser.add_argument(
        "--scanned_file_name", "-f", help="the name of the json file outputted by scancode", required=True
    )
    parser.add_argument("--mode", "-m", help="two modes: check or fix", required=True)

    args = parser.parse_args()

    scanned_file_name: str = args.scanned_file_name
    mode: str = args.mode

    requirements_list: list[str] = ["requirements.txt", "requirements-lint.txt", "requirements-tests.txt"]
    notice_file_name: str = "NOTICE.txt"

    np = NoticeParser(
        requirement_files=requirements_list,
        scanned_json_file=scanned_file_name,
        cli_mode_argument=mode,
        notice_fn=notice_file_name,
    )
