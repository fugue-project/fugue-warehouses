import os

from setuptools import find_packages, setup

from fugue_warehouses_version import __version__

with open("README.md") as f:
    LONG_DESCRIPTION = f.read()


def get_version() -> str:
    tag = os.environ.get("RELEASE_TAG", "")
    if "dev" in tag.split(".")[-1]:
        return tag
    if tag != "":
        assert tag == __version__, "release tag and version mismatch"
    return __version__


setup(
    name="fugue-warehouses",
    version=get_version(),
    packages=find_packages(),
    description="Fugue data warehouse integrations",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license="Apache-2.0",
    author="The Fugue Development Team",
    author_email="hello@fugue.ai",
    keywords="fugue data warehouses bigquery trino sql",
    url="http://github.com/fugue-project/fugue-warehouses",
    install_requires=[],
    extras_require={
        "bigquery": [
            "fugue[ibis]==0.8.1",
            "fs-gcsfs",
            "pandas-gbq",
            "google-auth",
            "ibis-framework[bigquery]",
        ],
        "ray": ["fugue[ray]==0.8.1"],
    },
    classifiers=[
        # "3 - Alpha", "4 - Beta" or "5 - Production/Stable"
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    python_requires=">=3.8",
    entry_points={
        "fugue.plugins": [
            "bigquery = fugue_bigquery.registry",
            "bigquery_ray = fugue_bigquery.ray_execution_engine[ray]",
        ],
        "ibis.backends": ["fugue_trino = fugue_trino.ibis_trino"],
    },
)
