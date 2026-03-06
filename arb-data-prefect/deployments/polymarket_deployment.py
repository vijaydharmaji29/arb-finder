"""Deployment configuration for Polymarket data fetching flow."""

import os
import sys
from pathlib import Path
from prefect import flow
from prefect_github import GitHubRepository

github_source = GitHubRepository(
    repository_url="https://github.com/vijaydharmaji29/arb-data-prefect.git",
    reference="main",  # branch, tag, or commit SHA
    access_token=os.getenv("GITHUB_TOKEN")
)


# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from prefect.schedules import Cron


if __name__ == "__main__":
    # Save the GitHub repository block before using it
    github_source.save("polymarket-github-repo", overwrite=True)
    
    deployment = flow.from_source(
            source=github_source,
            entrypoint="src/flows/fetch_polymarket_data.py:fetch_polymarket_data_flow",
        ).deploy(
            name="polymarket-5min",
            work_pool_name="arb-markets-pool",
            schedule=Cron("*/5 * * * *", timezone="UTC"),
        )

    print(deployment)

