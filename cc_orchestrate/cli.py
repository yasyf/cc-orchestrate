from __future__ import annotations

import shutil

import click

BACKENDS = ("cmux", "superset")


@click.group()
@click.version_option(package_name="cc-orchestrate")
def main() -> None:
    """Orchestrate fleets of Claude Code agents across pluggable backends like cmux and superset."""


@main.command()
def backends() -> None:
    """List supported backends and whether each is installed on this machine."""
    for name in BACKENDS:
        path = shutil.which(name)
        click.echo(f"{name}\t{'available' if path else 'not found'}")
