"""
"""
# External Libraries
import click


CONF_FILE = click.option(
    '--conf-file',
    '-c',
    metavar='CONF_FILE',
    type=click.types.Path(exists=True),
    default=None,
    help='configuration file path',
)
