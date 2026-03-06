"""
Prefect deployment script for arbitrage detection flow.
Deploys the flow from GitHub source using flow.from_source().deploy()

Usage:
    python deploy.py

Environment Variables (optional):
    GITHUB_REPO_URL: GitHub repository URL (default: prompts for input)
    WORK_POOL_NAME: Prefect work pool name (default: "default-agent-pool")
    DEPLOYMENT_NAME: Name for the deployment (default: "arbitrage-detection-deployment")
    GITHUB_BRANCH: Branch to deploy from (default: "main")
    GITHUB_ACCESS_TOKEN: Access token for private repositories (optional)
"""
import os
from prefect import flow
from dotenv import load_dotenv
from prefect_github import GitHubRepository, GitHubCredentials


load_dotenv()


def deploy_arbitrage_detection():
    """
    Deploy the arbitrage detection flow from GitHub source.
    """
    # Get configuration from environment variables or use defaults
    github_repo_url = "https://github.com/vijaydharmaji29/arb-analytics-prefect.git"
    
    if not github_repo_url:
        github_repo_url = input("Enter GitHub repository URL: ").strip()
    
    if not github_repo_url:
        raise ValueError("GITHUB_REPO_URL must be provided either as environment variable or input")
    
    work_pool_name = os.getenv("WORK_POOL_NAME", "arb-analytics-prefect-pool")
    deployment_name = os.getenv("DEPLOYMENT_NAME", "arb-analytics-deployment")
    github_branch = os.getenv("GITHUB_BRANCH", "main")
    github_access_token = os.getenv("GITHUB_TOKEN")
    
    print(f"Deploying flow from GitHub source...")
    print(f"  Repository: {github_repo_url}")
    print(f"  Branch: {github_branch}")
    print(f"  Work Pool: {work_pool_name}")
    print(f"  Deployment Name: {deployment_name}")
    
    # Prepare GitHub credentials if access token is provided
    github_credentials = None
    if github_access_token:
        github_credentials = GitHubCredentials(token=github_access_token)
    
    # Create GitHubRepository source
    source = GitHubRepository(
        repository_url=github_repo_url,
        reference=github_branch if github_branch else None,
        credentials=github_credentials
    )
    source.save("arb-analytics-prefect-source", overwrite=True)
    
    # Deploy flow from GitHub source
    deployment = flow.from_source(
        source=source,
        entrypoint="arbitrage_detection.py:detect_arbitrage_opportunities"
    ).deploy(
        name=deployment_name,
        work_pool_name=work_pool_name,
        description="Deployment for detecting arbitrage opportunities between Kalshi and Polymarket markets"
    )
    
    print(f"\n✓ Deployment '{deployment.name}' created successfully!")
    print(f"  Deployment ID: {deployment.id}")
    print(f"\nYou can now run this deployment using:")
    print(f"  prefect deployment run '{deployment_name}/{deployment.name}'")
    
    return deployment


if __name__ == "__main__":
    deploy_arbitrage_detection()

