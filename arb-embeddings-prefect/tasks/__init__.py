"""Prefect tasks for market embedding and matching."""
from .data_tasks import fetch_markets_data
from .embedding_tasks import (
    process_kalshi_markets_task,
    process_polymarket_markets_task,
    initialize_embedding_service_task
)
from .matching_tasks import (
    run_similarity_matching_task,
    save_matches_to_db_task,
    generate_report_task
)
from .database_tasks import get_db_connection_task

__all__ = [
    'fetch_markets_data',
    'process_kalshi_markets_task',
    'process_polymarket_markets_task',
    'initialize_embedding_service_task',
    'run_similarity_matching_task',
    'save_matches_to_db_task',
    'generate_report_task',
    'get_db_connection_task',
]

