#!/usr/bin/env python

import setuptools

requirements = ["apache-airflow"]

setuptools.setup(
    name="feat22",
    version="0.0.1",
    description="Package for demonstrating defferable operators",
    install_requires=requirements,
    packages=setuptools.find_packages(where="src", include=["feat22.mypkg"]),
    package_dir={"": "src"},
    license="MIT license",
)
