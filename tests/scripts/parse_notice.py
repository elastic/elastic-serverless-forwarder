# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
import re
from datetime import datetime

import requests
from requests import Response


class NoticeParser:
    def __init__(self, requirement_files, scanned_json_file):
        self.requirement_files = requirement_files
        self.scanned_json_file = scanned_json_file
        self.processed_modules = {}
        self.required_modules = {}
        self.notice_file_name = "NOTICE.txt"

        self.read_requirements()

        scanned_results_data = self.read_content_from_file(self.scanned_json_file)
        self.scanned_results_json = json.loads(scanned_results_data)

        notice_file_content = self.read_content_from_file("NOTICE.txt")
        module_pattern = r"(?:Module: ([^\n]+))"
        found_requirement_modules = re.findall(module_pattern, notice_file_content)
        found_requirement_modules.sort()

        original_requirement_names = [requirement for requirement in self.required_modules.values()]
        original_requirement_names.sort()

        if original_requirement_names == found_requirement_modules:
            raise ValueError("There is no new package listed in the requirements files\n")

        for new_module in original_requirement_names:
            if new_module not in found_requirement_modules and new_module.isascii():
                print(f"New module found: {new_module}\n")
                self.process_module(required_module=new_module)
                self.verify_license_in_modules()
                self.write_to_file(self.processed_modules[new_module])

    def process_module(self, required_module):
        for entry in self.scanned_results_json["files"]:
            if (
                entry["path"].endswith("LICENSE")
                or entry["path"].endswith("LICENSE.txt")
                or entry["path"].endswith("METADATA")
                or entry["path"].endswith("METADATA.txt")
            ) and len(entry["licenses"]) > 0:
                splitted_entry_path: list[str] = entry["path"].split("/")

                if not len(splitted_entry_path) > 6:
                    continue

                module_name = splitted_entry_path[5].split("-")[0]

                if module_name == required_module:
                    if module_name not in self.processed_modules:
                        self.processed_modules[module_name] = {}

                        self.processed_modules[module_name]["license_name"] = entry["licenses"][0]["key"].upper()

                        license_path: str = entry["path"][29:]
                        self.processed_modules[module_name]["license_path"] = license_path

                        self.processed_modules[module_name]["module_name"] = self.required_modules[required_module]

                    if "METADATA" in splitted_entry_path[-1]:
                        module_version: str = splitted_entry_path[5].split("-")[1].strip(".dist")
                        self.processed_modules[module_name]["version"] = module_version

                        homepage_url: str = entry["packages"][0]["homepage_url"]
                        vcs_url: str = entry["packages"][0]["vcs_url"]

                        if "github" not in homepage_url and vcs_url and "github" in vcs_url:
                            homepage_url = vcs_url.split(" ")[-1]

                        self.processed_modules[module_name]["homepage_url"] = homepage_url

                    if "LICENSE" in splitted_entry_path[-1]:
                        license_content: str = self.read_content_from_file(license_file_path=license_path)
                        self.processed_modules[module_name]["license_content"] = license_content
        else:
            raise ValueError(f"Nothing has been found for module '{required_module}' in the scanned file\n")

    @staticmethod
    def read_content_from_file(license_file_path: str) -> str:
        with open(license_file_path) as fh:
            license_content: str = fh.read()

        return license_content

    def read_requirements(self) -> None:
        for requirement_file in self.requirement_files:
            with open(requirement_file) as fh:
                req_data: list[str] = fh.readlines()

                for original_requirement in req_data:
                    module_name: str = original_requirement.split("=")[0].strip(">").strip("\n").replace("-", "_")

                    if module_name not in self.required_modules:
                        if "[" and "]" in module_name:
                            module_name: str = module_name.split("[")[0]

                        self.required_modules[module_name] = original_requirement.split("=")[0].strip(">").strip("\n")

    def verify_license_in_modules(self) -> None:
        for processed_module in self.processed_modules:
            if "license_content" not in self.processed_modules[processed_module]:
                try:
                    raw_github_base_url: str = "https://raw.githubusercontent.com"
                    homepage_url: str = self.processed_modules[processed_module]["homepage_url"]

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
                            self.processed_modules[processed_module]["license_content"] = response.text
                            self.processed_modules[processed_module]["license_path"] = github_page
                            break

                except Exception as e:
                    print(e)
                    self.processed_modules[processed_module]["license_content"] = ""

    def write_to_file(self, module_data: dict[str, str]) -> None:
        with open(self.notice_file_name, "a+") as fh:
            fh.write(f"Module: {module_data['module_name']}")
            fh.write("\n")
            fh.write(f"Version: {module_data['version']}")
            fh.write("\n")
            fh.write(f"Homepage: {module_data['homepage_url']}")
            fh.write("\n")
            fh.write(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
            fh.write("\n")
            fh.write(f"License: {module_data['license_name']}")
            fh.write("\n" * 2)
            fh.write(f"Contents of probable licence file {module_data['license_path']}: \n")
            fh.write("\n")
            fh.write(module_data["license_content"])
            fh.write("\n" * 2)
            fh.write("-" * 100)
            fh.write("\n")


if __name__ == "__main__":
    requirements_list: list[str] = ["requirements.txt", "requirements-lint.txt", "requirements-tests.txt"]
    scanned_file_name: str = "NOTICE.json"
    np = NoticeParser(requirement_files=requirements_list, scanned_json_file=scanned_file_name)

    for module in np.required_modules:
        np.process_module(required_module=module)

    np.verify_license_in_modules()

    for module in np.processed_modules:
        np.write_to_file(module_data=np.processed_modules[module])

