# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
from datetime import datetime

import requests


def main(scanned_json_output: str) -> None:
    seen_package: dict[str, dict[str, str]] = {}
    required_packages: dict[str, str] = {}

    read_requirements("requirements.txt", required_packages)
    read_requirements("requirements-lint.txt", required_packages)
    read_requirements("requirements-tests.txt", required_packages)

    requirements_list: list[str] = [key.strip("\n") for key in required_packages.keys()]

    with open(scanned_json_output) as fh:
        data = fh.read()
        dict_data = json.loads(data)

    for entry in dict_data["files"]:
        if (
            entry["path"].endswith("LICENSE")
            or entry["path"].endswith("LICENSE.txt")
            or entry["path"].endswith("METADATA")
            or entry["path"].endswith("METADATA.txt")
        ) and len(entry["licenses"]) > 0:
            splitted_entry_path: list[str] = entry["path"].split("/")

            if not len(splitted_entry_path) > 5:
                continue

            package = splitted_entry_path[5].split("-")[0]

            if package in requirements_list:
                if package not in seen_package:
                    seen_package[package] = {}

                    seen_package[package]["license_name"] = entry["licenses"][0]["key"].upper()

                    license_path: str = entry["path"][29:]
                    seen_package[package]["license_path"] = license_path

                if splitted_entry_path[-1].startswith("METADATA"):
                    package_version: str = splitted_entry_path[5].split("-")[1].strip(".dist")
                    seen_package[package]["version"] = package_version

                    homepage_url: str = entry["packages"][0]["homepage_url"]
                    vcs_url: str = entry["packages"][0]["vcs_url"]

                    if "github" not in homepage_url and vcs_url and "github" in vcs_url:
                        homepage_url = vcs_url.split(" ")[-1]

                    seen_package[package]["homepage_url"] = homepage_url

                if splitted_entry_path[-1].startswith("LICENSE"):

                    license_content: str = read_license_from_file(license_file_path=license_path)
                    seen_package[package]["license_content"] = license_content

    for package in seen_package:
        if "license_content" not in seen_package[package]:
            try:
                github_project = "/".join(homepage_url.split("/")[3:])
                github_license_pages: list[str] = [
                    f"https://raw.githubusercontent.com/{github_project}/master/LICENSE",
                    f"https://raw.githubusercontent.com/{github_project}/master/LICENSE.txt",
                    f"https://raw.githubusercontent.com/{github_project}/main/LICENSE",
                    f"https://raw.githubusercontent.com/{github_project}/main/LICENSE.txt",
                ]

                for github_page in github_license_pages:
                    response = requests.get(github_page)

                    if response.status_code == 200:
                        seen_package[package]["license_content"] = response.text
                        seen_package[package]["license_path"] = github_page

            except Exception as e:
                print(e)
                seen_package[package]["license_content"] = ""

        if len(seen_package) != len(requirements_list):
            raise AssertionError("There are missing packages")

        write_to_file(seen_package[package], "NOTICE.txt")


def read_requirements(file_name: str, packages: dict[str, str]) -> None:
    with open(file_name) as fh:
        req_data: list[str] = fh.readlines()

        for requirement in req_data:
            cleaned_requirement_name = requirement.split("=")[0].strip(">").replace("-", "_")

            if "[" and "]" in cleaned_requirement_name:
                cleaned_requirement_name = cleaned_requirement_name.split("[")[0]

            if cleaned_requirement_name not in packages:
                packages[cleaned_requirement_name] = ""


def write_to_file(data: dict[str, str], file_name: str) -> None:

    with open(file_name, "a+") as fh:
        fh.write(f"Module: {data['homepage_url']}")
        fh.write("\n")
        fh.write(f"Version: {data['version']}")
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


def read_license_from_file(license_file_path: str) -> str:
    with open(license_file_path) as fh:
        license_content: str = fh.read()

    return license_content


if __name__ == "__main__":
    main(scanned_json_output="NOTICE.json")
