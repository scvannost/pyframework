#!/usr/bin/env python

from distutils.core import setup

setup(
    name="pyFramework",
    version="0.1",
    description="Entity Framework for Python",
    license="mit",
    author="SC van Nostrand",
    author_email="scvannost@gmail.com",
    download_url="https://github.com/scvannost/pyframework/archive/v_0.1.tar.gz",
    url="https://scvannost.com",
    packages=[
        "pyframework",
        "pyframework.core",
    ],
    python_requires=">=3.0",
    install_requires=[
        "mysqlclient",
        "passlib",
    ],
    keywords=["DATABASE"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
    ],
)
