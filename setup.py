#!/usr/bin/env python
import sys
from setuptools import find_namespace_packages, setup

if sys.version_info < (3, 9) or sys.version_info >= (3, 13):
    print('Error: dbt-sap-hana-cloud does not support this version of Python.')
    print('Please install Python 3.9 or higher but less than 3.13.')
    sys.exit(1)

package_name = "dbt-sap-hana-cloud"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.0.0"
description = """The sap-hana-cloud adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Ambuj",
    author_email="ambuj.solanki@sap.com",
    url="https://github.com/SAP-samples/dbt-sap-hana-cloud",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core==1.9.0",
        "dbt-adapters>=1.7.2",
        "dbt-common>=1.3.0",
        "hdbcli~=2.22.32"
    ],
)
