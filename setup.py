from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="remotools",

    python_requires='>=3.7',

    version="0.0.1",

    author="Vlad Streltsin",

    author_email="vladstreltsin@gmail.com",

    description="Utilities for easy access to remote resources",

    long_description=long_description,

    long_description_content_type="text/markdown",

    url="https://github.com/vladstreltsin/remotools.git",

    find_packages=['remotools'],

    install_requires=[
        'tqdm>=4.51.0,<5',
        'xxhash>=2.0.0',
        'requests>=2.24.0',
        'sqlitedict>= 1.7.0',
        'cachetools',
        'scalpl'
    ],

    extras_require={
        # Remotes
        "gs": ['google-cloud-storage>=1.35.0'],
        "s3": ['boto3>=1.16.51', 'botocore>=1.19.51'],

        # Savers
        "PIL": ['Pillow>=8.0.1', 'numpy>=1.19.2'],
        "jsonpickle": ['jsonpickle>=1.4.2'],
        "plyfile": ['plyfile>=0.7.2'],
        "yaml": ['ruamel.yaml>=0.16.12'],
        "pandas": ['pandas>=0.24.2'],
        "torch": ['pytorch']
    }
)
