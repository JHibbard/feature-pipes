# Standard Libraries
from setuptools import setup, find_namespace_packages
import pathlib


APP_NAME = 'fpipes'
VERSION = '0.0.0'
AUTHOR = 'James Hibbard'
AUTHOR_EMAIL = ''
DESCRIPTION = ''
URL = ''


# Directory containing this file
HERE = pathlib.Path(__file__).parent

# Text of the README.md
README = (HERE / "README.md").read_text()

setup(
    name=APP_NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESCRIPTION,
    long_description=README,
    long_description_content_type="text/markdown",
    url=URL,
    install_requires=[
        "pyspark>=3.2.1",
        "delta-spark>=1.1.0",
        "click>=8.0.3",
    ],
    packages=find_namespace_packages("src"),
    package_dir={"": "src"},
    package_data={
        "": [
            ".yaml",
        ],
    },
    entry_points="""
    [console_scripts]
        fpipe=fpipe.cli:cli
    """,
    python_requires=">=3.8",
    classifiers=[
    ],
)
