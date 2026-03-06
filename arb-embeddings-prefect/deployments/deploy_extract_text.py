"""Deployment configuration for Extract Text flow."""

import os
import sys
from pathlib import Path
from prefect_github import GitHubRepository
from prefect.schedules import Cron
from dotenv import load_dotenv

load_dotenv()
github_source = GitHubRepository(
    repository_url="https://github.com/vijaydharmaji29/arb-embeddings-prefect.git",
    reference="main",  # branch, tag, or commit SHA
    access_token=os.getenv("GITHUB_TOKEN")
)

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


if __name__ == "__main__":
    # Save the GitHub repository block before using it
    github_source.save("extract-text-github-repo", overwrite=True)
    
    # Get deployment parameters from environment or defaults
    deploy_name = os.getenv("EXTRACT_TEXT_DEPLOYMENT_NAME", "extract-text-deployment")
    work_pool_name = os.getenv("WORK_POOL_NAME", "arb-embeddings-pool")
    
    # Schedule configuration
    schedule = None
    if os.getenv("EXTRACT_TEXT_CRON_SCHEDULE"):
        schedule = Cron(os.getenv("EXTRACT_TEXT_CRON_SCHEDULE"), timezone="UTC")
    elif os.getenv("EXTRACT_TEXT_INTERVAL_SECONDS"):
        from prefect.schedules import Interval
        schedule = Interval(seconds=int(os.getenv("EXTRACT_TEXT_INTERVAL_SECONDS")))
    
    deploy_kwargs = {
        "name": deploy_name,
        "work_pool_name": work_pool_name,
    }
    
    if schedule:
        deploy_kwargs["schedule"] = schedule
    
    if os.getenv("WORK_QUEUE_NAME"):
        deploy_kwargs["work_queue_name"] = os.getenv("WORK_QUEUE_NAME")
    
    print(f"Deploying extract_text flow with configuration:")
    for key, value in deploy_kwargs.items():
        print(f"  {key}: {value}")
    
    from prefect import flow
    
    deployment = flow.from_source(
        source=github_source,
        entrypoint="flows/extract_text_flow.py:extract_text_flow",
    ).deploy(**deploy_kwargs)
    
    print(f"Deployment complete!")
    print(deployment)

