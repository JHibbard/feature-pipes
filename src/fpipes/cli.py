"""Command Line Interface
"""
# Standard Libraries
import json
import os

# External Libraries
import click
import pyspark
from delta.pip_utils import configure_spark_with_delta_pip
from delta import DeltaTable

# Internal Libraries
from .utils.cli_args import CONF_FILE


CONTEXT_SETTINGS = dict(
    help_option_names=["--help"], max_content_width=80, auto_envvar_prefix="FPD"
)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
def cli():
    pass


@cli.command()
@CONF_FILE
@click.option(
    '--job',
    '-j',
    metavar="JOB",
    type=click.Choice([

    ], case_sensitive=False, ),
    hint='Job type to run'
)
def run(conf_file, job):
    """
    """
    # read config
    with open(conf_file, 'r') as file:
        config = json.load(file)
