from typing import List, TypedDict

import typer
from typer import Typer

from laceworkreports import common

from .AgentCoverageHandler import AgentCoverageHandler
from .ComplianceCoverageHandler import ComplianceCoverageHandler
from .ContainerIntegrationCoverageHandler import ContainerIntegrationCoverageHandler
from .ContainerVulnerabilityCoverageHandler import ContainerVulnerabilityCoverageHandler
from .InventoryCoverageHandler import InventoryCoverageHandler
from .VpcChartHandler import VpcChartHandler
from .VulnerabilityCoverageHandler import VulnerabilityCoverageHandler

app: Typer = Typer(no_args_is_help=True)


class Command(TypedDict):
    command_name: str
    command_type: Typer


commands: list[Command] = [
    {
        "command_name": "agent-coverage",
        "command_type": AgentCoverageHandler.app,
    },
    {
        "command_name": "compliance-coverage",
        "command_type": ComplianceCoverageHandler.app,
    },
    {
        "command_name": "vpc-chart",
        "command_type": VpcChartHandler.app,
    },
    {
        "command_name": "vulnerability-coverage",
        "command_type": VulnerabilityCoverageHandler.app,
    },
    {
        "command_name": "container-vulnerability-coverage",
        "command_type": ContainerVulnerabilityCoverageHandler.app,
    },
    {
        "command_name": "container-integration-coverage",
        "command_type": ContainerIntegrationCoverageHandler.app,
    },
    {
        "command_name": "inventory-coverage",
        "command_type": InventoryCoverageHandler.app,
    },
]


for command in commands:
    app.add_typer(
        command["command_type"],
        name=command["command_name"],
        help=f"Generate {command['command_name']} report",
        no_args_is_help=True,
        epilog=f"{common.config.name} {command['command_name']} agent-coverage <exporttype> [OPTIONS]",
    )


if __name__ == "__main__":
    app()
