"""Main entry point for running Prefect flows."""

from src.flows.fetch_all_data import fetch_all_data


if __name__ == "__main__":
    fetch_all_data()

