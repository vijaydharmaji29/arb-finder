# Table Update Optimization Guide

This document explains the different optimization methods available for updating the `market_matching` table with arbitrage prices.

## Performance Comparison

| Method | Speed | Best For | Notes |
|--------|-------|----------|-------|
| **temp_table** (default) | ⚡⚡⚡⚡⚡ Fastest | Large batches (1000+) | 10-100x faster than original |
| **unnest** | ⚡⚡⚡⚡ Very Fast | Medium-large batches | Single UPDATE statement |
| **execute_values** | ⚡⚡⚡ Fast | Medium batches | 5-10x faster than executemany |
| **execute_batch** | ⚡⚡ Fast | Small-medium batches | 3-5x faster than executemany |
| **executemany** (old) | ⚡ Slow | Not recommended | Original method (removed) |

## Method Details

### 1. temp_table (Default - Recommended)

**How it works:**
1. Creates a temporary table
2. Bulk inserts all updates using `execute_values`
3. Performs a single `UPDATE FROM JOIN` statement
4. Temporary table auto-drops on commit

**Advantages:**
- Fastest method for bulk updates
- Single UPDATE statement (minimal overhead)
- Works well with large datasets (10k+ records)
- Leverages PostgreSQL's optimized JOIN operations

**Usage:**
```python
# Set environment variable
export UPDATE_METHOD=temp_table

# Or use default (temp_table is already the default)
```

**Performance:** Typically 10-100x faster than individual UPDATE statements

---

### 2. unnest

**How it works:**
- Uses PostgreSQL's `unnest` function with arrays
- Single UPDATE statement with array parameters
- Very efficient for PostgreSQL

**Advantages:**
- Single UPDATE statement
- No temporary table needed
- Very fast for medium-large batches
- Clean SQL syntax

**Usage:**
```python
export UPDATE_METHOD=unnest
```

**Performance:** Typically 20-50x faster than individual UPDATE statements

---

### 3. execute_values

**How it works:**
- Uses `psycopg2.extras.execute_values`
- Efficient batch processing with VALUES clause
- Updates via JOIN with VALUES

**Advantages:**
- Faster than executemany
- Good for medium batches
- No temporary table overhead

**Usage:**
```python
export UPDATE_METHOD=execute_values
```

**Performance:** Typically 5-10x faster than executemany

---

### 4. execute_batch

**How it works:**
- Uses `psycopg2.extras.execute_batch`
- Optimized batch execution with prepared statements
- Processes in efficient page sizes

**Advantages:**
- Faster than executemany
- Good for small-medium batches
- Uses prepared statements for efficiency

**Usage:**
```python
export UPDATE_METHOD=execute_batch
```

**Performance:** Typically 3-5x faster than executemany

---

## Configuration

Set the `UPDATE_METHOD` environment variable to choose the optimization method:

```bash
# Fastest (default)
export UPDATE_METHOD=temp_table

# Very fast alternative
export UPDATE_METHOD=unnest

# Fast alternatives
export UPDATE_METHOD=execute_values
export UPDATE_METHOD=execute_batch
```

## Additional Optimization Tips

### 1. Database Indexes
Ensure you have an index on `market_matching.id`:
```sql
CREATE INDEX IF NOT EXISTS idx_market_matching_id ON market_matching(id);
```

### 2. Connection Pooling
For high-frequency updates, consider using connection pooling:
```python
from psycopg2 import pool
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 20, database_url)
```

### 3. Batch Size Tuning
The current implementation processes all records at once for maximum speed. If you need to process in chunks, you can modify the batch size in the helper functions.

### 4. Transaction Size
All methods use a single transaction for maximum speed. If you need smaller transactions, you can modify the code to commit in batches.

### 5. Parallel Processing
For extremely large datasets, consider splitting the work across multiple workers:
```python
# Split opportunities into chunks
chunks = [arbitrage_opportunities[i:i+10000] for i in range(0, len(arbitrage_opportunities), 10000)]
# Process chunks in parallel (requires additional setup)
```

## Benchmarking

To benchmark different methods, run:
```bash
# Test temp_table
export UPDATE_METHOD=temp_table
python arbitrage_detection.py

# Test unnest
export UPDATE_METHOD=unnest
python arbitrage_detection.py

# Compare results
```

The output will show:
- Total time
- Average time per record
- Records per second

## Recommended Settings

**For most use cases:** Use `temp_table` (default)
- Best overall performance
- Handles any batch size efficiently
- Most reliable

**For very large batches (100k+ records):** Use `temp_table` or `unnest`
- Both handle large datasets well
- `temp_table` may have slight edge for very large datasets

**For small batches (<1000 records):** Any method will work well
- Performance difference is minimal
- `execute_batch` or `execute_values` are fine

## Troubleshooting

### "relation already exists" error
- The temporary table should auto-drop on commit
- If you see this error, check for connection issues

### Memory issues with very large datasets
- Consider processing in chunks
- Use `execute_batch` with smaller page_size

### Slow performance
- Check database indexes
- Verify connection is not over network (use local if possible)
- Check PostgreSQL configuration (shared_buffers, work_mem)

