from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="remotools",
    version="0.0.1",
    author="Vlad Streltsin",
    author_email="vladstreltsin@gmail.com",
    description="Utilities for easy access to remote resources",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vladstreltsin/storage.git",
    find_packages=['remotools'],
    install_requires=[]
)