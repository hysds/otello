import click

from otello.client import initialize


@click.group()
def cli():
    """Wrapper library to communicate with HySDS"""


@cli.command()
def init():
    """Initialize the config file (config.yml) in ~/.config/otello"""
    initialize()


# TODO: add ci cli functionality
# TODO: add mozart cli functionality
