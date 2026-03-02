"""Setup script for WoS Beam Pipeline package."""

import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="wos-beam-pipeline",
    version="1.0.0",
    author="Web of Science Data Team",
    description="Production-ready Dataflow pipeline for processing WoS XML to BigQuery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
    install_requires=[
        "apache-beam[gcp]==2.53.0",
        "google-cloud-storage==2.14.0",
        "google-cloud-bigquery==3.14.1",
        "lxml==5.1.0",
        "python-dateutil==2.8.2",
    ],
    extras_require={
        "dev": [
            "pytest==7.4.3",
            "pytest-cov==4.1.0",
        ],
    },
)
