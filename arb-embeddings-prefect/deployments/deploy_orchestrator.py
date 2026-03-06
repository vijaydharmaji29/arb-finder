"""Deployment configuration for Orchestrator flow."""

import os
import sys
from pathlib import Path
from prefect_github import GitHubRepository
from prefect.schedules import Cron

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
    github_source.save("orchestrator-github-repo", overwrite=True)
    
    # Get deployment parameters from environment or defaults
    deploy_name = os.getenv("ORCHESTRATOR_DEPLOYMENT_NAME", "orchestrator-deployment")
    work_pool_name = os.getenv("WORK_POOL_NAME", "arb-embeddings-pool")
    
    # Schedule configuration
    schedule = None
    if os.getenv("ORCHESTRATOR_CRON_SCHEDULE"):
        schedule = Cron(os.getenv("ORCHESTRATOR_CRON_SCHEDULE"), timezone="UTC")
    elif os.getenv("ORCHESTRATOR_INTERVAL_SECONDS"):
        from prefect.schedules import Interval
        schedule = Interval(seconds=int(os.getenv("ORCHESTRATOR_INTERVAL_SECONDS")))
    
    deploy_kwargs = {
        "name": deploy_name,
        "work_pool_name": work_pool_name,
    }
    
    if schedule:
        deploy_kwargs["schedule"] = schedule
    
    if os.getenv("WORK_QUEUE_NAME"):
        deploy_kwargs["work_queue_name"] = os.getenv("WORK_QUEUE_NAME")
    
    # Add parameters
    params = {}
    if os.getenv("SIMILARITY_THRESHOLD"):
        params["similarity_threshold"] = float(os.getenv("SIMILARITY_THRESHOLD"))
    if params:
        deploy_kwargs["parameters"] = params
    
    print(f"Deploying orchestrator flow with configuration:")
    for key, value in deploy_kwargs.items():
        print(f"  {key}: {value}")
    
    from prefect import flow
    
    deployment = flow.from_source(
        source=github_source,
        entrypoint="flows/orchestrator_flow.py:orchestrator_flow",
    ).deploy(**deploy_kwargs)
    
    print(f"Deployment complete!")
    print(deployment)

