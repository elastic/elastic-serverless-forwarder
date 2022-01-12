# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
import re
from datetime import datetime

import requests
from requests import Response


def main(scanned_json_output: str) -> None:
    processed_modules: dict[str, dict[str, str]] = {}
    required_modules: dict[str, str] = {}

    read_requirements("requirements.txt", required_modules)
    read_requirements("requirements-lint.txt", required_modules)
    read_requirements("requirements-tests.txt", required_modules)

    requirements_list: list[str] = [key for key in required_modules.keys()]
    requirements_list.sort()

    original_required_modules_list: list[str] = [value for value in required_modules.values()]
    original_required_modules_list.sort()

    notice_file_content = read_content_from_file("NOTICE.txt")

    module_pattern = r"(?:Module: ([^\n]+))"
    found_requirement_modules = re.findall(module_pattern, notice_file_content)
    found_requirement_modules.sort()

    if original_required_modules_list == found_requirement_modules:
        print("There is no new package listed in the requirements files")
        return

    for new_module in original_required_modules_list:
        if not new_module.isascii():
            print(f"New module: '{new_module}' is not valid")
            return
        if new_module not in found_requirement_modules and len(found_requirement_modules) > 1:
            print(f"New module added: {new_module}")
            print("You need scan the project's files and provide the results in the NOTICE.json file")
            return

    process_module(scanned_json_output, processed_modules, required_modules)

    if len(original_required_modules_list) != len(requirements_list):
        raise AssertionError("There are missing packages")

    for module in processed_modules:
        if "license_content" not in processed_modules[module]:
            try:
                raw_github_base_url: str = "https://raw.githubusercontent.com"
                homepage_url: str = processed_modules[module]["homepage_url"]

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
                        processed_modules[module]["license_content"] = response.text
                        processed_modules[module]["license_path"] = github_page

            except Exception as e:
                print(e)
                processed_modules[module]["license_content"] = ""

        write_to_file(processed_modules[module], "NOTICE.txt")


def process_module(
    scanned_json_output_file: str, processed_modules: dict[str, dict[str, str]], required_modules
) -> None:
    scanned_results_data = read_content_from_file(scanned_json_output_file)
    scanned_results_json = json.loads(scanned_results_data)

    for entry in scanned_results_json["files"]:
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

            if module_name in [key for key in required_modules.keys()]:
                if module_name not in processed_modules:
                    processed_modules[module_name] = {}

                    processed_modules[module_name]["license_name"] = entry["licenses"][0]["key"].upper()

                    license_path: str = entry["path"][29:]
                    processed_modules[module_name]["license_path"] = license_path

                    processed_modules[module_name]["module"] = required_modules[module_name]

                if "METADATA" in splitted_entry_path[-1]:
                    package_version: str = splitted_entry_path[5].split("-")[1].strip(".dist")
                    processed_modules[module_name]["version"] = package_version

                    homepage_url: str = entry["packages"][0]["homepage_url"]
                    vcs_url: str = entry["packages"][0]["vcs_url"]
                    extra_info: str = entry["licenses"][0]["scancode_text_url"]

                    if "github" not in homepage_url and vcs_url and "github" in vcs_url:
                        homepage_url = vcs_url.split(" ")[-1]

                    processed_modules[module_name]["homepage_url"] = homepage_url
                    processed_modules[module_name]["extra_info"] = extra_info

                if "LICENSE" in splitted_entry_path[-1]:
                    license_content: str = read_content_from_file(license_file_path=license_path)
                    processed_modules[module_name]["license_content"] = license_content


def read_requirements(file_name: str, packages: dict[str, str]) -> None:
    with open(file_name) as fh:
        req_data: list[str] = fh.readlines()

        for requirement in req_data:
            module_name: str = requirement.split("=")[0].strip(">").strip("\n").replace("-", "_")

            if module_name not in packages:
                if "[" and "]" in module_name:
                    module_name: str = module_name.split("[")[0]

                packages[module_name] = requirement.split("=")[0].strip(">").strip("\n")


def write_to_file(data: dict[str, str], file_name: str) -> None:

    with open(file_name, "a+") as fh:
        fh.write(f"Module: {data['module']}")
        fh.write("\n")
        fh.write(f"Version: {data['version']}")
        fh.write("\n")
        fh.write(f"Homepage: {data['homepage_url']}")
        fh.write("\n")
        fh.write(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
        fh.write("\n")
        fh.write(f"License: {data['license_name']}")
        fh.write("\n" * 2)
        fh.write(f"Contents of probable licence file {data['license_path']}: \n")
        fh.write("\n")
        fh.write(data["license_content"])
        fh.write("\n" * 2)
        fh.write("-" * 100)
        fh.write("\n")


def read_content_from_file(license_file_path: str) -> str:
    with open(license_file_path) as fh:
        license_content: str = fh.read()

    return license_content


if __name__ == "__main__":
    main(scanned_json_output="NOTICE.json")
