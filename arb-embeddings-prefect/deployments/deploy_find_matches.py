"""Deployment configuration for Find Matches flow."""

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
    github_source.save("find-matches-github-repo", overwrite=True)
    
    # Get deployment parameters from environment or defaults
    deploy_name = os.getenv("FIND_MATCHES_DEPLOYMENT_NAME", "find-matches-deployment")
    work_pool_name = os.getenv("WORK_POOL_NAME", "arb-embeddings-pool")
    
    # Schedule configuration
    schedule = None
    if os.getenv("FIND_MATCHES_CRON_SCHEDULE"):
        schedule = Cron(os.getenv("FIND_MATCHES_CRON_SCHEDULE"), timezone="UTC")
    elif os.getenv("FIND_MATCHES_INTERVAL_SECONDS"):
        from prefect.schedules import Interval
        schedule = Interval(seconds=int(os.getenv("FIND_MATCHES_INTERVAL_SECONDS")))
    
    deploy_kwargs = {
        "name": deploy_name,
        "work_pool_name": work_pool_name,
    }
    
    if schedule:
        deploy_kwargs["schedule"] = schedule
    
    if os.getenv("WORK_QUEUE_NAME"):
        deploy_kwargs["work_queue_name"] = os.getenv("WORK_QUEUE_NAME")
    
    # Add parameters - load_all_markets=True to run on all markets
    params = {
        "load_all_markets": True  # Load all markets regardless of embedding_created status
    }
    if os.getenv("SIMILARITY_THRESHOLD"):
        params["similarity_threshold"] = float(os.getenv("SIMILARITY_THRESHOLD"))
    if os.getenv("GENERATE_REPORT", "true").lower() == "false":
        params["generate_report"] = False
    if os.getenv("OUTPUT_FILE"):
        params["output_file"] = os.getenv("OUTPUT_FILE")
    
    deploy_kwargs["parameters"] = params
    
    print(f"Deploying find_matches flow with configuration:")
    for key, value in deploy_kwargs.items():
        print(f"  {key}: {value}")
    
    from prefect import flow
    
    deployment = flow.from_source(
        source=github_source,
        entrypoint="flows/find_matches_flow.py:find_matches_flow",
    ).deploy(**deploy_kwargs)
    
    print(f"Deployment complete!")
    print(deployment)

