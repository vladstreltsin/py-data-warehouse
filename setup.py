from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

# TODO xxhash as extra install option

setup(
    name="remotools",

    python_requires='>=3.7',

    version="0.0.1",

    author="Vlad Streltsin",

    author_email="vladstreltsin@gmail.com",

    description="Utilities for easy access to remote resources",

    long_description=long_description,

    long_description_content_type="text/markdown",

    url="https://github.com/vladstreltsin/storage.git",

    find_packages=['remotools'],

    extras_require=
    {
        "gs": ['google-cloud-storage>=1.35.0'],
        "s3": ['boto3>=1.16.51', 'botocore>=1.19.51']
    }
)