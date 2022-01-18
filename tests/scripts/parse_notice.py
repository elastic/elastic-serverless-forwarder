# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
import re
import sys
from datetime import datetime
from typing import Any

import requests
from requests import Response


class NoticeParser:
    POSSIBLE_LICENSE_FILES: list[str] = [
        "LICENSE",
        "LICENSE.txt",
        "LICENSE.srt",
        "apache-2.0.LICENSE",
        "apache-2.0.LICENSE.txt",
    ]
    POSSIBLE_METADATA_FILES: list[str] = ["METADATA", "METADATA.txt"]

    def __init__(self, requirement_files: list[str], scanned_json_file: str) -> None:
        self.requirement_files: list[str] = requirement_files
        self.scanned_json_file: str = scanned_json_file
        self.processed_packages: dict[str, dict[str, str]] = {}
        self.required_packages: dict[str, str] = {}
        self.notice_file_name: str = "NOTICE.txt"

        self.read_requirements()

        scanned_results_data = self.read_content_from_file(self.scanned_json_file)
        self.scanned_results_json: Any = json.loads(scanned_results_data)

        notice_file_content: str = self.read_content_from_file("NOTICE.txt")

        package_pattern = r"(?:Package: ([^\n]+))"
        existing_packages: list[str] = re.findall(package_pattern, notice_file_content)
        existing_packages.sort()

        requirements_name_from_file = [requirement for requirement in self.required_packages.keys()]
        requirements_name_from_file.sort()

        real_requirements_name = [requirement for requirement in self.required_packages.values()]
        real_requirements_name.sort()

        if real_requirements_name == existing_packages:
            print("There is no new package listed in the requirements files\n")
            sys.exit()

        for new_package in requirements_name_from_file:
            if self.required_packages[new_package] not in existing_packages:
                real_package_name: str = self.required_packages[new_package]

                print(f"New package found: '{real_package_name}'")
                self.process_package(required_package=new_package)
                self.verify_license_in_packages()

                processed_package = self.processed_packages.get(new_package)

                if not processed_package:
                    print(f"Nothing has been found for package '{real_package_name}' in the scanned file\n")
                    continue

                if (
                    "package_name" not in processed_package
                    or "version" not in processed_package
                    or "homepage_url" not in processed_package
                    or "license_name" not in processed_package
                    or "license_path" not in processed_package
                    or "license_content" not in processed_package
                ):
                    print(f"Missing data for '{real_package_name}'. Skipping...\n")
                    continue

                self.write_to_file(processed_package)
                print(f"Package '{real_package_name}' has been added to {self.notice_file_name}\n")

    def process_package(self, required_package: str) -> None:
        """
        Iterates over the json file outputted by scancode and tries to find a match between
        the required package and the installed one and looks in the self.POSSIBLE_LICENSE_FILES
        and self.POSSIBLE_METADATA_FILES where the important information about the package should exist
        """
        for entry in self.scanned_results_json["files"]:
            splitted_entry_path: list[str] = entry["path"].split("/")

            if (
                splitted_entry_path[-1] in NoticeParser.POSSIBLE_LICENSE_FILES
                or splitted_entry_path[-1] in NoticeParser.POSSIBLE_METADATA_FILES
            ):
                if not len(splitted_entry_path) > 6:
                    continue

                package_name = splitted_entry_path[5].split("-")[0]

                if package_name == required_package:
                    if package_name not in self.processed_packages:
                        self.processed_packages[package_name] = {}
                        self.processed_packages[package_name]["package_name"] = self.required_packages[required_package]

                        if entry["licenses"] and len(entry["licenses"]) > 0:
                            self.processed_packages[package_name]["license_name"] = entry["licenses"][0]["key"].upper()

                    if splitted_entry_path[-1] in NoticeParser.POSSIBLE_METADATA_FILES:
                        package_version: str = splitted_entry_path[5].split("-")[1].strip(".dist")
                        self.processed_packages[package_name]["version"] = package_version

                        homepage_url: str = entry["packages"][0]["homepage_url"]
                        vcs_url: str = entry["packages"][0]["vcs_url"]

                        if not homepage_url and not vcs_url:
                            print("No Homepage URL or VCS URL for the license. Skipping...")
                            return

                        if "github" not in homepage_url and vcs_url and "github" in vcs_url:
                            homepage_url = vcs_url.split(" ")[-1]

                        self.processed_packages[package_name]["homepage_url"] = homepage_url

                    if splitted_entry_path[-1] in NoticeParser.POSSIBLE_LICENSE_FILES:
                        license_path: str = entry["path"][29:]
                        self.processed_packages[package_name]["license_path"] = license_path

                        license_content: str = self.read_content_from_file(content_file_path=license_path)
                        self.processed_packages[package_name]["license_content"] = license_content

    @staticmethod
    def read_content_from_file(content_file_path: str) -> str:
        """
        Reads a file and returns the string representation of its content
        """
        with open(content_file_path) as fh:
            license_content: str = fh.read()

        return license_content

    def read_requirements(self) -> None:
        """
        Reads the inputted requirements and creates the self.required_pacakges dict that contains
        parsed package name as key and original package name as value (eg. "elastic_apm": "elastic-apm")

        We need the parsed version of the package because the folder name where the package exists
        has "_" instead "-" (eg. "venv/.../elastic_apm-6.7.2.dist-info/...)
        """
        for requirement_file in self.requirement_files:
            with open(requirement_file) as fh:
                req_data: list[str] = fh.readlines()

                for original_requirement in req_data:
                    package_name: str = original_requirement.split("=")[0].strip(">").strip("\n").replace("-", "_")

                    if package_name not in self.required_packages:
                        if "[" and "]" in package_name:
                            package_name = package_name.split("[")[0]

                        self.required_packages[package_name] = original_requirement.split("=")[0].strip(">").strip("\n")

    def verify_license_in_packages(self) -> None:
        """
        Checks if the license_content exists for all packages
        If license not found, it tries to build a URL for a possible location where the LICENSE may be found
        """
        for processed_package in self.processed_packages:
            if "license_content" not in self.processed_packages[processed_package]:
                try:
                    raw_github_base_url: str = "https://raw.githubusercontent.com"
                    homepage_url: str = self.processed_packages[processed_package]["homepage_url"]

                    github_project: str = "/".join(homepage_url.split("/")[3:])
                    github_license_pages: list[str] = [
                        f"{raw_github_base_url}/{github_project}/master/LICENSE",
                        f"{raw_github_base_url}/{github_project}/master/LICENSE.txt",
                        f"{raw_github_base_url}/{github_project}/main/LICENSE",
                        f"{raw_github_base_url}/{github_project}/main/LICENSE.txt",
                    ]

                    for github_page in github_license_pages:
                        response: Response = requests.get(github_page)

                        if response.status_code == 200:
                            self.processed_packages[processed_package]["license_content"] = response.text
                            self.processed_packages[processed_package]["license_path"] = github_page
                            break
                        else:
                            print(f"License could not be found at: {github_page}")

                except Exception as e:
                    print(e)
                    self.processed_packages[processed_package]["license_content"] = ""

    def write_to_file(self, package_data: dict[str, str]) -> None:
        """
        Writes the NOTICE.txt file with the package data
        """
        with open(self.notice_file_name, "a+") as fh:
            fh.write("\n" * 2)
            fh.write("-" * 100)
            fh.write("\n")
            fh.write(f"Package: {package_data['package_name']}")
            fh.write("\n")
            fh.write(f"Version: {package_data['version']}")
            fh.write("\n")
            fh.write(f"Homepage: {package_data['homepage_url']}")
            fh.write("\n")
            fh.write(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
            fh.write("\n")
            fh.write(f"License: {package_data['license_name']}")
            fh.write("\n" * 2)
            fh.write(f"Contents of probable licence file {package_data['license_path']}: \n")
            fh.write("\n")
            fh.write(package_data["license_content"])


if __name__ == "__main__":
    requirements_list: list[str] = ["requirements.txt", "requirements-lint.txt", "requirements-tests.txt"]
    scanned_file_name: str = "NOTICE.json"

    np = NoticeParser(requirement_files=requirements_list, scanned_json_file=scanned_file_name)
