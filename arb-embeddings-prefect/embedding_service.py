"""Embedding service module using Pinecone vector storage."""
import os
import time
from typing import List, Dict, Optional, Callable
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class PineconeStorageMixin:
    """Mixin for cloud vector storage using Pinecone with integrated embeddings."""
    
    def _ensure_storage_initialized(self):
        """Lazy initialization of Pinecone storage."""
        if hasattr(self, '_storage_initialized') and self._storage_initialized:
            return
        
        # Lazy imports - only import when actually needed
        try:
            from pinecone import Pinecone
        except ImportError:
            raise ImportError(
                "pinecone package is not installed. Install it with: pip install pinecone"
            )
        
        from sentence_transformers import CrossEncoder
        
        # Load environment variables
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        if not os.path.exists(env_path):
            env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(dotenv_path=env_path)
        
        # Get Pinecone configuration
        self.pinecone_api_key = getattr(self, 'pinecone_api_key', None) or os.getenv('PINECONE_API_KEY')
        self.pinecone_index_name = getattr(self, 'pinecone_index_name', None) or os.getenv('PINECONE_INDEX_NAME', 'arb-prediction-market-data')
        self.pinecone_embedding_model = getattr(self, 'pinecone_embedding_model', None) or os.getenv('PINECONE_EMBEDDING_MODEL', 'llama-text-embed-v2')
        self.pinecone_cloud = getattr(self, 'pinecone_cloud', None) or os.getenv('PINECONE_CLOUD', 'aws')
        self.pinecone_region = getattr(self, 'pinecone_region', None) or os.getenv('PINECONE_REGION', 'us-east-1')
        
        # Initialize Pinecone reranker model name (optional, falls back to cross-encoder if not provided)
        # Check if already set in __init__, otherwise get from env or use default
        if not hasattr(self, 'pinecone_rerank_model') or self.pinecone_rerank_model is None:
            self.pinecone_rerank_model = os.getenv('PINECONE_RERANK_MODEL', 'bge-reranker-v2-m3')
        
        # Keep cross-encoder as optional fallback (only load if Pinecone reranking is not available)
        self.rerank_model_name = getattr(self, 'rerank_model_name', None) or os.getenv('RERANK_MODEL')
        self.rerank_model = None
        if self.rerank_model_name:
            print(f"Loading cross-encoder fallback model: {self.rerank_model_name}...")
            self.rerank_model = CrossEncoder(self.rerank_model_name)
        else:
            print(f"Using Pinecone native reranking with model: {self.pinecone_rerank_model}")
        
        if not self.pinecone_api_key:
            raise ValueError("PINECONE_API_KEY environment variable is required")
        
        # Initialize Pinecone client
        print(f"Initializing Pinecone client...")
        self.pinecone_client = Pinecone(api_key=self.pinecone_api_key)
        
        # Check if index exists, create if needed
        existing_indexes = [idx.name for idx in self.pinecone_client.list_indexes()]
        
        if self.pinecone_index_name not in existing_indexes:
            print(f"Index '{self.pinecone_index_name}' does not exist. Creating new index with integrated embeddings...")
            try:
                self.pinecone_client.create_index_for_model(
                    name=self.pinecone_index_name,
                    cloud=self.pinecone_cloud,
                    region=self.pinecone_region,
                    embed={
                        "model": self.pinecone_embedding_model,
                        "field_map": {"text": "text"}  # Map 'text' field for embedding
                    }
                )
                print(f"Created index '{self.pinecone_index_name}' with integrated embedding model: {self.pinecone_embedding_model}")
            except Exception as e:
                print(f"Warning: create_index_for_model failed: {e}")
                print("Attempting to create regular index...")
                from pinecone import ServerlessSpec
                self.pinecone_client.create_index(
                    name=self.pinecone_index_name,
                    dimension=1536,  # Default dimension
                    metric='cosine',
                    spec=ServerlessSpec(cloud=self.pinecone_cloud, region=self.pinecone_region)
                )
                print(f"Created regular index '{self.pinecone_index_name}' (you'll need to provide embeddings manually)")
        else:
            print(f"Using existing index '{self.pinecone_index_name}'")
        
        # Connect to index
        self.pinecone_index = self.pinecone_client.Index(self.pinecone_index_name)
        
        # Mark as initialized
        self._storage_initialized = True
        
        # Print embedding counts at initialization
        self._print_embedding_stats()
    
    def _initialize_storage(self):
        """Initialize Pinecone storage (deprecated - use _ensure_storage_initialized)."""
        self._ensure_storage_initialized()
    
    def _print_embedding_stats(self):
        """Print the number of embeddings in each namespace."""
        try:
            if not hasattr(self, 'pinecone_index') or self.pinecone_index is None:
                return
            stats = self.pinecone_index.describe_index_stats()
            namespaces = stats.get('namespaces', {}) or stats.get('namespace_stats', {})
            print("=== Pinecone Vector Store Statistics ===")
            total = 0
            if namespaces:
                for namespace, ns_stats in namespaces.items():
                    count = ns_stats.get('vector_count', 0) if isinstance(ns_stats, dict) else 0
                    total += count
                    print(f"  Namespace '{namespace}': {count} embeddings")
            else:
                # Try to get total count
                total = stats.get('total_vector_count', 0)
                print(f"  Total embeddings: {total}")
            if not namespaces and total == 0:
                print("  No embeddings found (empty store)")
            print("=" * 40)
        except Exception as e:
            print(f"Warning: Could not get embedding stats: {e}")
    
    def _store_vectors(self, records: List[Dict], namespace: Optional[str] = None,
                      batch_callback: Optional[Callable[[List[str]], None]] = None):
        """Store embeddings in Pinecone - Pinecone generates embeddings automatically from text."""
        if not records:
            return
        
        self._ensure_storage_initialized()
        namespace = namespace or 'markets'
        
        print(f"Processing {len(records)} records for Pinecone upload...")
        
        # Prepare records for Pinecone upload
        # Pinecone with integrated embeddings expects records with '_id', 'text', and metadata fields
        pinecone_records = []
        all_market_ids = []
        
        for record in records:
            record_id = record['id']
            metadata = record.get('metadata', {})
            
            # Build Pinecone record - text field will be automatically embedded by Pinecone
            pinecone_record = {
                '_id': record_id,
                'text': record['text'],  # Pinecone will generate embeddings from this
            }
            
            # Add metadata fields at top level (Pinecone requires primitive types)
            for key, value in metadata.items():
                if isinstance(value, (str, int, float, bool)):
                    pinecone_record[key] = value
                elif isinstance(value, list):
                    pinecone_record[key] = [str(item) for item in value]
                else:
                    pinecone_record[key] = str(value)
            
            pinecone_records.append(pinecone_record)
            
            market_id = metadata.get('market_id', '')
            if market_id:
                all_market_ids.append(market_id)
            
            # Track uploaded markets for similarity matching
            self.uploaded_markets.append({
                'record_id': record_id,
                'market_type': metadata.get('market_type', ''),
                'market_id': market_id,
                'title': metadata.get('title', ''),
                'category': metadata.get('category', '')
            })

        
        # Upload to Pinecone in batches
        batch_size = 96
        total_uploaded = 0
        num_batches = (len(pinecone_records) + batch_size - 1) // batch_size
        
        max_retries = 3
        retry_delay = 60  # Initial delay in seconds
        
        for i in range(0, len(pinecone_records), batch_size):
            batch = pinecone_records[i:i + batch_size]
            batch_num = i // batch_size + 1
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    self.pinecone_index.upsert_records(namespace, batch)
                    total_uploaded += len(batch)
                    print(f"  Uploaded batch {batch_num}/{num_batches}: {total_uploaded}/{len(pinecone_records)} records")
                    success = True
                except Exception as e:
                    retry_count += 1
                    # Check if this is a metadata size error and truncate if needed
                    error_str = str(e)
                    if "Metadata size" in error_str and "exceeds the limit" in error_str:
                        print(f"\n  DEBUG: Metadata size exceeded, truncating large records in batch {batch_num}:")
                        import json
                        truncated_any = False
                        for idx, record in enumerate(batch):
                            record_size = len(json.dumps(record).encode('utf-8'))
                            if record_size > 35000:  # Truncate if close to 40KB limit
                                print(f"    Record {idx} (id={record.get('_id', 'unknown')}): {record_size} bytes - TRUNCATING")
                                # Truncate the 'text' field which is typically the largest
                                if 'text' in record and len(record['text']) > 2000:
                                    record['text'] = record['text'][:2000] + "... [truncated]"
                                    truncated_any = True
                                # Also truncate description if present
                                if 'description' in record and len(str(record.get('description', ''))) > 1000:
                                    record['description'] = str(record['description'])[:1000] + "... [truncated]"
                                    truncated_any = True
                                # Truncate rules_primary if present
                                if 'rules_primary' in record and len(str(record.get('rules_primary', ''))) > 1000:
                                    record['rules_primary'] = str(record['rules_primary'])[:1000] + "... [truncated]"
                                    truncated_any = True
                                new_size = len(json.dumps(record).encode('utf-8'))
                                print(f"      New size after truncation: {new_size} bytes")
                        
                        if truncated_any:
                            print(f"  Retrying batch {batch_num} immediately with truncated records...")
                            retry_count -= 1  # Don't count this as a retry since we fixed the issue
                            continue  # Retry immediately without delay
                    
                    if retry_count < max_retries:
                        delay = retry_delay * (2 ** (retry_count - 1))  # Exponential backoff
                        print(f"  Warning: Failed to upsert batch {batch_num} (attempt {retry_count}/{max_retries}): {e}")
                        print(f"  Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        print(f"  Error: Failed to upsert batch {batch_num} after {max_retries} attempts: {e}")
                        print(f"  Stopping upload process. Uploaded {total_uploaded}/{len(pinecone_records)} records before failure.")
                        return
        
        # Call callback once with all market IDs (uses temp table for bulk update)
        if batch_callback and all_market_ids:
            try:
                batch_callback(all_market_ids)
            except Exception as e:
                print(f"  Warning: Callback failed: {e}")
        
        print(f"Saved {len(records)} vectors to Pinecone (namespace: {namespace})")
        print("Embeddings were automatically generated by Pinecone")
    
    def _update_metadata(self, record_ids: List[str], metadata_update: Dict, namespace: Optional[str] = None):
        """
        Update metadata for records in Pinecone.
        
        Args:
            record_ids: List of record IDs to update
            metadata_update: Dictionary of metadata fields to update
            namespace: Namespace containing the records
        """
        self._ensure_storage_initialized()
        namespace = namespace or 'markets'
        
        if not record_ids:
            return
        
        updated_count = 0
        failed_count = 0

        print("Skipping metadata update for ", len(record_ids), " records")
        
        for record_id in record_ids:
            try:
                self.pinecone_index.update(
                    id=record_id,
                    set_metadata=metadata_update,
                    namespace=namespace
                )
                updated_count += 1
            except Exception as e:
                print(f"  Warning: Failed to update metadata for {record_id}: {e}")
                failed_count += 1
        
        if updated_count > 0:
            print(f"  Updated metadata for {updated_count} records in Pinecone")
        if failed_count > 0:
            print(f"  Failed to update {failed_count} records")
    
    def _query_similar(self, record_id: str, top_k: int, namespace: Optional[str] = None,
                      filter_dict: Optional[Dict] = None) -> List[Dict]:
        """
        Query for similar vectors using Pinecone's native similarity search.
        
        Uses Pinecone's optimized similarity search with cosine distance.
        Results are automatically sorted by similarity (highest first).
        """
        self._ensure_storage_initialized()
        namespace = namespace or 'markets'
        
        try:
            # First, fetch the record to get its text for querying
            fetch_result = self.pinecone_index.fetch(ids=[record_id], namespace=namespace)
            
            if not fetch_result.get('vectors') or record_id not in fetch_result['vectors']:
                return []
            
            # Get metadata to construct query text
            record_metadata = fetch_result['vectors'][record_id].get('metadata', {})
            query_text = record_metadata.get('text', '')
            
            if not query_text:
                # Construct query text from metadata
                title = record_metadata.get('title', '')
                description = record_metadata.get('description', '')
                subtitle = record_metadata.get('subtitle', '')
                yes_subtitle = record_metadata.get('yes_subtitle', '')
                no_subtitle = record_metadata.get('no_subtitle', '')
                rules_primary = record_metadata.get('rules_primary', '')
                
                text_parts = []
                if title:
                    text_parts.append(str(title))
                if subtitle:
                    text_parts.append(str(subtitle))
                if yes_subtitle:
                    text_parts.append(str(yes_subtitle))
                if no_subtitle:
                    text_parts.append(str(no_subtitle))
                if description:
                    text_parts.append(str(description))
                if rules_primary:
                    text_parts.append(str(rules_primary))
                query_text = ' '.join(text_parts).strip()
            
            # For Pinecone with integrated embeddings, we can query by text directly
            # Pinecone filter format: simple dict with field: value for equality
            # Convert filter_dict to Pinecone format if needed
            pinecone_filter = None
            if filter_dict:
                # Pinecone uses simple equality filters: {"field": "value"}
                # Handle both simple dict and nested dict formats
                pinecone_filter = {}
                for key, value in filter_dict.items():
                    if isinstance(value, dict):
                        # Handle nested format like {'$eq': 'value'}
                        if '$eq' in value:
                            pinecone_filter[key] = value['$eq']
                        elif '$ne' in value:
                            # Pinecone uses $ne for not equal
                            pinecone_filter[key] = {"$ne": value['$ne']}
                        else:
                            pinecone_filter[key] = value
                    else:
                        pinecone_filter[key] = value
            
            # Query with Pinecone native reranking using search() method
            # Use a higher top_k initially to get more candidates for reranking
            initial_top_k = max(top_k * 5, 50)  # Get more candidates for reranking
            try:
                # Build search query with Pinecone native reranking
                search_params = {
                    'namespace': namespace,
                    'query': {
                        'id': record_id,
                        'top_k': initial_top_k + 1, # +1 because the query record itself will be in results,
                        'filter': pinecone_filter if pinecone_filter else None
                    },
                    'rerank': {
                        'model': 'cohere-rerank-3.5',
                        'rank_fields': ['text'],
                        'top_n': top_k + 1,  # Return top_n after reranking (+1 to account for query record)
                        'query': query_text  # Query text for reranking
                    },
                    'fields': ['text']  # Fields to return
                }
                
                # Add filter if provided (Pinecone search supports filter in query)
                if pinecone_filter:
                    search_params['query']['filter'] = pinecone_filter
                
                query_results = self.pinecone_index.search(**search_params)
                
            except Exception as e:
                # Check if error is due to reranking not being supported, fallback to non-reranked query
                error_str = str(e).lower()
                if 'rerank' in error_str or 'reranking' in error_str or 'search' in error_str:
                    print(f"Warning: Pinecone reranking/search not available, falling back to query method: {e}")
                    # Fallback: use query method without reranking
                    try:
                        query_results = self.pinecone_index.query(
                            data=query_text,
                            top_k=top_k + 1,
                            namespace=namespace,
                            filter=pinecone_filter if pinecone_filter else None,
                            include_metadata=True
                        )
                    except Exception as e2:
                        # Final fallback: try with vector if text query fails
                        query_vector = fetch_result['vectors'][record_id].get('values')
                        if query_vector:
                            query_results = self.pinecone_index.query(
                                vector=query_vector,
                                top_k=top_k + 1,
                                namespace=namespace,
                                filter=pinecone_filter if pinecone_filter else None,
                                include_metadata=True
                            )
                        else:
                            raise e2
                else:
                    # Other error, try fallback
                    query_vector = fetch_result['vectors'][record_id].get('values')
                    if query_vector:
                        query_results = self.pinecone_index.query(
                            vector=query_vector,
                            top_k=top_k + 1,
                            namespace=namespace,
                            filter=pinecone_filter if pinecone_filter else None,
                            include_metadata=True
                        )
                    else:
                        raise e
            
            # Process results from Pinecone search
            results = []
            
            # Handle Pinecone search() response format: result.hits
            # Also handle fallback query() response format: matches
            hits = []
            if 'result' in query_results and 'hits' in query_results['result']:
                # Pinecone search() format with reranking
                hits = query_results['result']['hits']
            elif 'matches' in query_results:
                # Fallback query() format
                hits = query_results['matches']
            elif 'results' in query_results:
                # Alternative format
                hits = query_results['results']
            
            if hits:
                for hit in hits:
                    # Skip the query record itself
                    match_id = hit.get('_id') or hit.get('id', '')
                    if match_id == record_id:
                        continue
                    
                    # Get score - reranked results have rerank score in _score
                    score = hit.get('_score') or hit.get('score', 0.0)
                    similarity = float(score)
                    
                    # Get fields and metadata
                    # Pinecone search() returns fields in 'fields' key
                    fields = hit.get('fields', {})
                    metadata = hit.get('metadata', {})
                    
                    # Get text from fields (search() returns fields separately)
                    text = fields.get('text', '') or metadata.get('text', '')
                    
                    # If text not in fields, try to construct from metadata
                    if not text:
                        title = metadata.get('title', '')
                        description = metadata.get('description', '')
                        subtitle = metadata.get('subtitle', '')
                        yes_subtitle = metadata.get('yes_subtitle', '')
                        no_subtitle = metadata.get('no_subtitle', '')
                        rules_primary = metadata.get('rules_primary', '')
                        
                        text_parts = []
                        if title:
                            text_parts.append(str(title))
                        if subtitle:
                            text_parts.append(str(subtitle))
                        if yes_subtitle:
                            text_parts.append(str(yes_subtitle))
                        if no_subtitle:
                            text_parts.append(str(no_subtitle))
                        if description:
                            text_parts.append(str(description))
                        if rules_primary:
                            text_parts.append(str(rules_primary))
                        text = ' '.join(text_parts).strip()
                    
                    # Merge fields into metadata for consistency
                    combined_metadata = {**metadata, **fields}
                    
                    results.append({
                        'id': match_id,
                        'score': similarity,
                        'metadata': combined_metadata,
                        'text': text
                    })
            
            # Results from Pinecone search with reranking are already reranked and sorted
            # The score from reranked results is the rerank score (already normalized)
            # Ensure results are sorted by score descending (should already be sorted by Pinecone)
            results.sort(key=lambda x: x['score'], reverse=True)
            
            # Apply top_k (results are already limited by rerank top_n, but apply again for safety)
            return results[:top_k] if top_k else results
            
        except Exception as e:
            print(f"Warning: Pinecone similarity search failed for {record_id}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _rerank_results(self, query_text: str, candidates: List[Dict], top_k: Optional[int] = None) -> List[Dict]:
        """
        Rerank candidate results using a cross-encoder model.
        
        Args:
            query_text: The query text to match against
            candidates: List of candidate dicts with 'id', 'score', 'metadata', and optionally 'text'
            top_k: Number of top results to return after reranking (None = return all)
            
        Returns:
            List of reranked candidates sorted by rerank score (highest first)
        """
        if not candidates:
            return []
        
        self._ensure_storage_initialized()
        
        # Prepare pairs for cross-encoder: (query_text, candidate_text)
        pairs = []
        candidate_texts = []
        
        for candidate in candidates:
            # Get candidate text from metadata or use a default
            candidate_text = candidate.get('text', '')
            if not candidate_text:
                # Try to construct text from metadata
                metadata = candidate.get('metadata', {})
                title = metadata.get('title', '')
                description = metadata.get('description', '')
                subtitle = metadata.get('subtitle', '')
                yes_subtitle = metadata.get('yes_subtitle', '')
                no_subtitle = metadata.get('no_subtitle', '')
                rules_primary = metadata.get('rules_primary', '')
                
                text_parts = []
                if title:
                    text_parts.append(str(title))
                if subtitle:
                    text_parts.append(str(subtitle))
                if yes_subtitle:
                    text_parts.append(str(yes_subtitle))
                if no_subtitle:
                    text_parts.append(str(no_subtitle))
                if description:
                    text_parts.append(str(description))
                if rules_primary:
                    text_parts.append(str(rules_primary))
                
                candidate_text = ' '.join(text_parts).strip()
            
            if candidate_text:
                pairs.append([query_text, candidate_text])
                candidate_texts.append(candidate_text)
            else:
                # If no text available, use empty string (will get low score)
                pairs.append([query_text, ''])
                candidate_texts.append('')
        
        # Get rerank scores from cross-encoder
        try:
            import numpy as np
            rerank_scores = self.rerank_model.predict(pairs)
            # Convert numpy array to list if needed
            if hasattr(rerank_scores, 'tolist'):
                rerank_scores = rerank_scores.tolist()
        except Exception as e:
            print(f"Warning: Reranking failed: {e}")
            # Return original candidates if reranking fails
            return candidates
        
        # Update candidates with rerank scores
        reranked_candidates = []
        import numpy as np
        for candidate, rerank_score in zip(candidates, rerank_scores):
            # Use rerank score as the new score (cross-encoder scores are typically in [-inf, inf])
            # Normalize to [0, 1] using sigmoid for better interpretability
            # Handle numpy scalars
            if hasattr(rerank_score, 'item'):
                rerank_score = rerank_score.item()
            normalized_score = 1 / (1 + np.exp(-float(rerank_score)))  # Sigmoid normalization
            
            reranked_candidate = candidate.copy()
            reranked_candidate['score'] = float(normalized_score)
            reranked_candidate['rerank_score'] = float(rerank_score)
            reranked_candidate['original_score'] = candidate.get('score', 0.0)
            reranked_candidates.append(reranked_candidate)
        
        # Sort by rerank score (descending)
        reranked_candidates.sort(key=lambda x: x['score'], reverse=True)
        
        # Return top_k if specified
        if top_k is not None:
            return reranked_candidates[:top_k]
        
        return reranked_candidates


class PineconeEmbeddingService(PineconeStorageMixin):
    """Embedding service using Pinecone as backend."""
    
    def __init__(self,
                 pinecone_api_key: Optional[str] = None,
                 pinecone_index_name: Optional[str] = None,
                 pinecone_embedding_model: Optional[str] = None,
                 pinecone_cloud: Optional[str] = None,
                 pinecone_region: Optional[str] = None,
                 rerank_model_name: Optional[str] = None,
                 pinecone_rerank_model: Optional[str] = None):
        """Initialize with Pinecone storage backend."""
        self.pinecone_api_key = pinecone_api_key
        self.pinecone_index_name = pinecone_index_name
        self.pinecone_embedding_model = pinecone_embedding_model
        self.pinecone_cloud = pinecone_cloud
        self.pinecone_region = pinecone_region
        self.rerank_model_name = rerank_model_name
        self.pinecone_rerank_model = pinecone_rerank_model
        self.similarity_matches: List[Dict] = []
        self.uploaded_markets: List[Dict] = []
        # Storage will be initialized lazily when first needed
        # This prevents blocking imports during initialization


class EmbeddingService:
    """
    Embedding service using Pinecone vector storage.
    
    Usage:
        # Using Pinecone with default configuration
        service = EmbeddingService()
        
        # Using Pinecone with custom index name
        service = EmbeddingService(pinecone_index_name='my-index')
    """
    
    def __init__(self, 
                 rerank_model_name: Optional[str] = None,
                 pinecone_api_key: Optional[str] = None,
                 pinecone_index_name: Optional[str] = None,
                 pinecone_embedding_model: Optional[str] = None,
                 pinecone_cloud: Optional[str] = None,
                 pinecone_region: Optional[str] = None,
                 pinecone_rerank_model: Optional[str] = None):
        """
        Initialize embedding service with Pinecone storage backend.
        
        Args:
            rerank_model_name: Cross-encoder model name for fallback reranking (optional, only used if Pinecone reranking fails)
            pinecone_api_key: Pinecone API key (default: from PINECONE_API_KEY env var)
            pinecone_index_name: Pinecone index name (default: from PINECONE_INDEX_NAME env var or 'arb-prediction-market-data')
            pinecone_embedding_model: Embedding model for Pinecone (default: from PINECONE_EMBEDDING_MODEL env var or 'llama-text-embed-v2')
            pinecone_cloud: Cloud provider ('aws' or 'gcp', default: from PINECONE_CLOUD env var or 'aws')
            pinecone_region: Region for the index (default: from PINECONE_REGION env var or 'us-east-1')
            pinecone_rerank_model: Pinecone reranker model name (default: from PINECONE_RERANK_MODEL env var or 'bge-reranker-v2-m3')
        """
        self.similarity_matches: List[Dict] = []
        self.uploaded_markets: List[Dict] = []
        
        self._backend = PineconeEmbeddingService(
            pinecone_api_key=pinecone_api_key,
            pinecone_index_name=pinecone_index_name,
            pinecone_embedding_model=pinecone_embedding_model,
            pinecone_cloud=pinecone_cloud,
            pinecone_region=pinecone_region,
            rerank_model_name=rerank_model_name,
            pinecone_rerank_model=pinecone_rerank_model
        )
        
        # Share state with backend
        self._backend.similarity_matches = self.similarity_matches
        self._backend.uploaded_markets = self.uploaded_markets
    
    def store_embeddings(self, records: List[Dict], namespace: Optional[str] = None,
                        batch_callback: Optional[Callable[[List[str]], None]] = None):
        """
        Store embeddings in the configured storage backend.
        
        Args:
            records: List of dicts with 'id', 'text', and optionally 'metadata'
            namespace: Optional namespace for the vectors
            batch_callback: Optional callback after each batch is processed
        """
        self._backend._store_vectors(records, namespace=namespace, batch_callback=batch_callback)
        # Sync state
        self.uploaded_markets = self._backend.uploaded_markets
    
    def _find_similar_markets(self, market_info: Dict, namespace: Optional[str] = None, threshold: float = 0.65, max_matches_per_market: int = 5):
        """Find similar markets in the opposite market_type for a given market.
        
        Uses a two-stage approach:
        1. Vector similarity search to get initial candidates (no threshold, get many candidates)
        2. Cross-encoder reranking to improve match quality
        3. Apply threshold and max_matches AFTER reranking for best results
        
        Args:
            market_info: Market information dictionary
            namespace: Namespace for vector storage
            threshold: Similarity threshold (applied AFTER reranking, default: 0.65)
            max_matches_per_market: Maximum number of matches to keep per market after reranking (default: 5)
        """
        market_type = market_info.get('market_type', '')
        record_id = market_info.get('record_id', '')
        
        if not market_type or not record_id:
            return
        
        opposite_type = 'polymarket' if market_type == 'kalshi' else 'kalshi'
        
        try:
            # First stage: Get initial candidates using vector similarity
            # Use a higher top_k to get more candidates for reranking (no threshold yet)
            # This allows reranking to find matches that might have lower vector similarity
            # but higher semantic relevance
            initial_top_k = 50  # Get candidates for reranking
            # Pinecone filter format: simple dict with field: value
            results = self._backend._query_similar(
                record_id=record_id,
                top_k=initial_top_k,  # Get 50 candidates, no threshold filtering yet
                namespace=namespace,
                filter_dict={'market_type': opposite_type}  # Pinecone uses simple equality
            )
            
            # Results are already reranked by _query_similar (sorted by rerank score descending)
            # Now apply threshold AFTER reranking (this is the correct order)
            # The reranked scores are more accurate than vector similarity scores
            matches_after_threshold = []
            for match in results:
                # Use rerank score (already normalized to [0, 1])
                score = match.get('score', 0.0)
                
                # Apply threshold AFTER reranking for best results
                if score >= threshold:
                    match_metadata = match.get('metadata', {})
                    
                    similarity_match = {
                        'source_market_type': market_type,
                        'source_market_id': market_info.get('market_id', ''),
                        'source_title': market_info.get('title', ''),
                        'target_market_type': opposite_type,
                        'target_market_id': match_metadata.get('market_id', ''),
                        'target_title': match_metadata.get('title', ''),
                        'similarity_score': score,
                        'source_category': market_info.get('category', ''),
                        'target_category': match_metadata.get('category', '')
                    }
                    matches_after_threshold.append(similarity_match)
                        
                        
            
            # Apply max_matches_per_market limit AFTER reranking and threshold
            # This ensures we keep only the best reranked matches
            if max_matches_per_market > 0:
                matches_after_threshold = matches_after_threshold[:max_matches_per_market]
            
            # Add all matches that passed threshold and max_matches limit
            self.similarity_matches.extend(matches_after_threshold)
        except Exception as e:
            print(f"Warning: Failed to find similar markets for {record_id}: {e}")
    
    def load_all_markets_from_pinecone(self, namespace: Optional[str] = None, skip_matched: bool = True):
        """
        Load all markets from Pinecone into uploaded_markets for matching.
        This allows matching on all markets regardless of embedding_created status.
        
        Args:
            namespace: Namespace to load from (default: "markets")
            skip_matched: If True, skip markets that already have match_found=True (default: True)
        """
        if namespace is None:
            namespace = "markets"
        
        try:
            self._backend._ensure_storage_initialized()
            index = self._backend.pinecone_index
            
            # Get all vectors from Pinecone using list operation
            # Note: Pinecone doesn't have a direct "get all" operation, so we'll use query with a dummy vector
            # or fetch by IDs if we have them. For now, we'll use stats to get count and then query.
            
            # Get stats to see what's available
            stats = index.describe_index_stats()
            namespaces = stats.get('namespaces', {}) or stats.get('namespace_stats', {})
            
            if namespace not in namespaces:
                print(f"No markets found in Pinecone namespace '{namespace}'")
                return
            
            # Clear existing uploaded_markets
            self.uploaded_markets = []
            
            # Fetch all markets from Pinecone using fetch_by_metadata with pagination
            # Use a filter that matches all records (checking for existence of market_type field)
            print(f"Fetching all markets from Pinecone namespace '{namespace}'...")
            if skip_matched:
                print("  (Skipping markets with match_found=True)")
            
            try:
                # Fetch all records with pagination - using $exists filter to match all records with market_type
                # Limit is 10,000 per call, so we paginate if there are more
                pagination_token = None
                page_count = 0
                total_fetched = 0
                skipped_matched = 0
                
                while True:
                    page_count += 1
                    
                    # Build fetch parameters
                    fetch_params = {
                        "namespace": namespace,
                        "limit": 100
                    }
                    
                    # Add pagination token if we have one
                    if pagination_token:
                        fetch_params["pagination_token"] = pagination_token
                    
                    # Fetch a page of record IDs
                    fetch_response = index.list_paginated(**fetch_params)
                    
                    # Extract record IDs from results
                    if hasattr(fetch_response, 'vectors'):
                        vectors_list = fetch_response.vectors
                        page_size = len(vectors_list)
                        
                        # Get the IDs from this page
                        page_ids = [row['id'] for row in vectors_list]
                        
                        # Fetch metadata for these IDs to check match_found status
                        if page_ids:
                            try:
                                metadata_response = index.fetch(ids=page_ids, namespace=namespace)
                                vectors_with_metadata = metadata_response.get('vectors', {})
                                
                                for record_id in page_ids:
                                    record_data = vectors_with_metadata.get(record_id, {})
                                    metadata = record_data.get('metadata', {})
                                    
                                    # Check if this market already has a match
                                    if skip_matched and metadata.get('match_found', False):
                                        skipped_matched += 1
                                        continue
                                    
                                    # Build uploaded_markets entry with metadata
                                    market_entry = {
                                        'record_id': record_id,
                                        'market_type': metadata.get('market_type') or ("kalshi" if record_id.startswith("kalshi") else "polymarket"),
                                        'market_id': metadata.get('market_id') or record_id.split("_")[1],
                                        'title': metadata.get('title', ''),
                                        'category': metadata.get('category', ''),
                                        'match_found': metadata.get('match_found', False)
                                    }
                                    self.uploaded_markets.append(market_entry)
                            except Exception as fetch_meta_error:
                                print(f"  Warning: Failed to fetch metadata for page {page_count}: {fetch_meta_error}")
                                # Fallback to just IDs without metadata
                                for row in vectors_list:
                                    market_entry = {
                                        'record_id': row['id'],
                                        'market_type': "kalshi" if row['id'].startswith("kalshi") else "polymarket",
                                        'market_id': row['id'].split("_")[1],
                                    }
                                    self.uploaded_markets.append(market_entry)
                        
                        total_fetched += page_size
                        print(f"  Fetched page {page_count}: {page_size} markets (total: {total_fetched}, loaded: {len(self.uploaded_markets)}, skipped: {skipped_matched})")
                    else:
                        # Handle different response formats
                        print(f"Warning: Unexpected response format from fetch_by_metadata")
                        print(f"Response type: {type(fetch_response)}")
                        if hasattr(fetch_response, '__dict__'):
                            print(f"Response attributes: {dir(fetch_response)}")

                    if fetch_response.pagination:
                        pagination_token = fetch_response.pagination.next
                    else:
                        break
                    break
                
                print(f"Loaded {len(self.uploaded_markets)} markets from Pinecone ({page_count} page(s))")
                if skipped_matched > 0:
                    print(f"  Skipped {skipped_matched} markets with existing matches")
                
                    
            except Exception as fetch_error:
                print(f"Warning: fetch_by_metadata failed: {fetch_error}")
                print(f"  Falling back to empty uploaded_markets list")
                import traceback
                traceback.print_exc()
            
            # Sync with backend
            self._backend.uploaded_markets = self.uploaded_markets
            
        except Exception as e:
            print(f"Warning: Failed to load markets from Pinecone: {e}")
            import traceback
            traceback.print_exc()
    
    
    def run_similarity_matching(self, namespace: Optional[str] = None, threshold: float = 0.65, max_matches_per_market: int = 5, source_market_type: str = 'kalshi', mark_matched: bool = False):
        """
        Run similarity matching for uploaded markets in one direction only.
        
        Args:
            namespace: Namespace to query (default: "markets")
            threshold: Similarity threshold (default 0.65 = 65%) - applied AFTER reranking
            max_matches_per_market: Maximum number of matches to keep per market after reranking (default: 5)
            source_market_type: Market type to use as source for matching ('kalshi' or 'polymarket'). 
                                Only markets of this type will be processed. Default: 'kalshi'
            mark_matched: If True, mark matched markets in Pinecone with match_found=True (default: True)
        """
        if namespace is None:
            namespace = "markets"
        
        # Sync state
        self.uploaded_markets = self._backend.uploaded_markets
        
        if not self.uploaded_markets:
            print("No markets uploaded. Skipping similarity matching.")
            return
        
        # Filter to only process markets of the source type (one-directional matching)
        source_markets = [m for m in self.uploaded_markets if m.get('market_type', '').lower() == source_market_type.lower()]
        
        if not source_markets:
            print(f"No {source_market_type} markets found in uploaded_markets. Skipping similarity matching.")
            return
        
        opposite_type = 'polymarket' if source_market_type.lower() == 'kalshi' else 'kalshi'
        print(f"\nRunning one-directional similarity matching: {source_market_type} -> {opposite_type}")
        print(f"Processing {len(source_markets)} {source_market_type} markets (out of {len(self.uploaded_markets)} total)...")
        print(f"Finding similar markets with threshold >= {threshold:.0%}...")
        print(f"Keeping top {max_matches_per_market} match(es) per market after reranking")
        
        # Track matched record IDs for marking in Pinecone
        matched_source_ids = set()
        matched_target_ids = set()
        initial_match_count = len(self.similarity_matches)
        
        total_markets = len(source_markets)
        for idx, market_info in enumerate(source_markets, 1):
            prev_matches = len(self.similarity_matches)
            self._find_similar_markets(
                market_info, 
                namespace=namespace, 
                threshold=threshold,
                max_matches_per_market=max_matches_per_market
            )
            
            # Track if this market found any matches
            if len(self.similarity_matches) > prev_matches:
                matched_source_ids.add(market_info.get('record_id', ''))
                # Track target market IDs too
                for match in self.similarity_matches[prev_matches:]:
                    target_type = match.get('target_market_type', '')
                    target_id = match.get('target_market_id', '')
                    if target_type and target_id:
                        target_record_id = f"{target_type}_{target_id}"
                        matched_target_ids.add(target_record_id)
            
            if idx % 50 == 0 or idx == total_markets:
                print(f"Processed {idx}/{total_markets} markets... ({len(self.similarity_matches)} matches found so far)")
        
        new_matches = len(self.similarity_matches) - initial_match_count
        print(f"Similarity matching complete. Found {new_matches} new matches with similarity >= {threshold:.0%}")
        
        # Mark matched markets in Pinecone
        if mark_matched and (matched_source_ids or matched_target_ids):
            all_matched_ids = list(matched_source_ids | matched_target_ids)
            print(f"\nMarking {len(all_matched_ids)} markets as matched in Pinecone...")
            self._backend._update_metadata(
                record_ids=all_matched_ids,
                metadata_update={'match_found': True},
                namespace=namespace
            )
    
    def get_market_matches_for_db(self) -> List[tuple]:
        """Get market matches in format ready for database batch insert.
        
        Returns:
            List of tuples, each containing (kalshi_market_id, polymarket_market_id, confidence)
        """
        if not self.similarity_matches:
            return []
        
        matches = []
        pair_scores = {}  # Track maximum confidence for each pair
        
        for match in self.similarity_matches:
            if match['source_market_type'] == 'kalshi':
                kalshi_id = match['source_market_id']
                polymarket_id = match['target_market_id']
            else:
                kalshi_id = match['target_market_id']
                polymarket_id = match['source_market_id']
            
            if not kalshi_id or not polymarket_id:
                continue
            
            pair_key = (str(kalshi_id), str(polymarket_id))
            confidence = match.get('similarity_score', 0.0)
            
            # Keep the maximum confidence score for each pair
            if pair_key not in pair_scores or confidence > pair_scores[pair_key]:
                pair_scores[pair_key] = confidence
                    
        # Convert to list of tuples with confidence
        for (kalshi_id, polymarket_id), confidence in pair_scores.items():
            matches.append((kalshi_id, polymarket_id, confidence))
        
        return matches
    
    def get_similarity_table(self) -> pd.DataFrame:
        """Generate a table of similar markets matching Kalshi to Polymarket markets."""
        if not self.similarity_matches:
            return pd.DataFrame(columns=[
                'Kalshi Market ID', 'Kalshi Title', 'Polymarket Market ID', 
                'Polymarket Title', 'Similarity Score', 'Kalshi Category', 'Polymarket Category'
            ])
        
        df = pd.DataFrame(self.similarity_matches)
        
        table_data = []
        for _, row in df.iterrows():
            if row['source_market_type'] == 'kalshi':
                table_data.append({
                    'Kalshi Market ID': row['source_market_id'],
                    'Kalshi Title': row['source_title'],
                    'Polymarket Market ID': row['target_market_id'],
                    'Polymarket Title': row['target_title'],
                    'Similarity Score': f"{row['similarity_score']:.2%}",
                    'Kalshi Category': row['source_category'],
                    'Polymarket Category': row['target_category']
                })
            else:
                table_data.append({
                    'Kalshi Market ID': row['target_market_id'],
                    'Kalshi Title': row['target_title'],
                    'Polymarket Market ID': row['source_market_id'],
                    'Polymarket Title': row['source_title'],
                    'Similarity Score': f"{row['similarity_score']:.2%}",
                    'Kalshi Category': row['target_category'],
                    'Polymarket Category': row['source_category']
                })
        
        result_df = pd.DataFrame(table_data)
        result_df = result_df.drop_duplicates(subset=['Kalshi Market ID', 'Polymarket Market ID'])
        result_df['Similarity Score Num'] = result_df['Similarity Score'].str.rstrip('%').astype(float)
        result_df = result_df.sort_values('Similarity Score Num', ascending=False)
        result_df = result_df.drop('Similarity Score Num', axis=1)
        
        return result_df
    
    def process_markets(self, markets: List[Dict], market_type: str, 
                       db_connection=None, table_name: Optional[str] = None, 
                       id_column: Optional[str] = None) -> List[str]:
        """
        Process markets to create embeddings in batches of 1000.
        
        After every 1000 embeddings are created, commits to Pinecone and updates the database.
        Uses the extracted_text field directly if available (populated by extract_text_flow).
        Falls back to building text from market fields if extracted_text is not available.
        
        Args:
            markets: List of market dictionaries with relevant fields
            market_type: Type of market ('kalshi' or 'polymarket')
            db_connection: Optional database connection for batch updates
            table_name: Optional table name for database updates
            id_column: Optional ID column name for database updates
            
        Returns:
            List of market IDs that were successfully processed
        """
        if not markets:
            return []
        
        batch_size = 1000
        all_processed_ids = []
        total_markets = len(markets)
        
        print(f"Processing {total_markets} {market_type} markets in batches of {batch_size}...")
        
        # Process markets in batches of 1000
        for batch_start in range(0, total_markets, batch_size):
            batch_end = min(batch_start + batch_size, total_markets)
            batch_markets = markets[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (total_markets + batch_size - 1) // batch_size
            
            print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch_markets)} markets)...")
            
            records = []
            batch_processed_ids = []
            
            for market in batch_markets:
                title = market.get('title') or ''
                description = market.get('description') or ''
                subtitle = market.get('subtitle') or ''
                yes_subtitle = market.get('yes_subtitle') or ''
                no_subtitle = market.get('no_subtitle') or ''
                rules_primary = market.get('rules_primary') or ''
                category = market.get('category') or ''

                final_text = f"Title: {title} Description: {description} Subtitle: {subtitle} Yes Subtitle: {yes_subtitle} No Subtitle: {no_subtitle} Rules Primary: {rules_primary} Category: {category}"
                
                # Check if extracted_text is available (populated by extract_text_flow)
                extracted_text = market.get('extracted_text')
                final_text = "Context:\n" + extracted_text + " Sub Context:\n" + extracted_text
                text = final_text
                
                
                market_id = None
                for possible_id in ['id', 'market_id', 'kalshi_market_id', 'polymarket_market_id']:
                    if possible_id in market:
                        market_id = market[possible_id]
                        break
                
                if not market_id:
                    print(f"Warning: No ID found for market with title: {title[:50]}...")
                    continue
                
                market_id_str = str(market_id)
                record_id = f"{market_type}_{market_id_str}"

                record = {
                    'id': str(record_id),
                    'text': text,
                    'metadata': {
                        'market_type': market_type,
                        'market_id': market_id_str,
                        'title': str(title) if title else '',
                        'description': str(description) if description else '',
                        'subtitle': str(subtitle) if subtitle else '',
                        'yes_subtitle': str(yes_subtitle) if yes_subtitle else '',
                        'no_subtitle': str(no_subtitle) if no_subtitle else '',
                        'rules_primary': str(rules_primary) if rules_primary else '',
                        'category': str(category) if category else '',
                    }
                }
                
                records.append(record)
                batch_processed_ids.append(market_id_str)
            
            # Store embeddings for this batch and update database
            if records:
                try:
                    batch_callback = None
                    if db_connection and table_name and id_column:
                        def update_batch(market_ids: List[str]):
                            try:
                                updated_count = db_connection.batch_update_embedding_status(
                                    table_name, 
                                    market_ids,
                                    id_column=id_column
                                )
                                print(f"Updated {updated_count} rows in {table_name} (batch {batch_num})")
                            except Exception as e:
                                print(f"    ✗ Warning: Failed to update {table_name} for batch {batch_num}: {e}")
                        batch_callback = update_batch
                    
                    # Store embeddings for this batch (commits to Pinecone)
                    self.store_embeddings(records, namespace="markets", batch_callback=batch_callback)
                    print(f"Successfully stored {len(records)} embeddings for batch {batch_num}/{total_batches}")
                    
                    all_processed_ids.extend(batch_processed_ids)
                except Exception as e:
                    print(f"  ✗ Error storing embeddings for batch {batch_num}: {e}")
                    # Continue with next batch even if this one fails
                    continue
            else:
                print(f"  No valid records in batch {batch_num}")
        
        print(f"\nCompleted processing {len(all_processed_ids)}/{total_markets} {market_type} markets")
        return all_processed_ids
