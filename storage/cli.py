"""CLI entrypoint for Storage."""

import click

from .cli_conflicts import conflicts
from .cli_debug import debug
from .cli_mount import configure, info, mount


@click.group()
def main():
    """Storage - A meta-filesystem with local cache and N-way replication"""
    pass


main.add_command(mount)
main.add_command(configure)
main.add_command(info)
main.add_command(debug)
main.add_command(conflicts)
main.add_command(conflicts, name="conflict")


if __name__ == "__main__":
    main()
