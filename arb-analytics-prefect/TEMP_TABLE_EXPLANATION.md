# How Temporary Tables Make Bulk Updates Fast

## The Problem: Why Individual UPDATEs Are Slow

### Old Approach (Slow):
```python
# For 10,000 records, this executes 10,000 separate UPDATE statements:
for record in arbitrage_opportunities:
    cur.execute("""
        UPDATE market_matching
        SET arb_price = %s
        WHERE id = %s
    """, (record['arb_price'], record['id']))
```

**What happens for each UPDATE:**
1. **Parse SQL** - PostgreSQL parses the UPDATE statement
2. **Plan query** - Creates execution plan (checks indexes, decides strategy)
3. **Find row** - Searches for the row using WHERE clause
4. **Lock row** - Acquires row-level lock
5. **Update row** - Modifies the data
6. **Write WAL** - Writes to Write-Ahead Log (for transaction safety)
7. **Release lock** - Releases the row lock
8. **Network round-trip** - Sends result back to application

**For 10,000 records:** This happens **10,000 times** = massive overhead!

---

## The Solution: Temporary Table + Single UPDATE

### New Approach (Fast):
```python
# Step 1: Create temporary table (happens once)
CREATE TEMPORARY TABLE temp_arb_updates (
    id INTEGER PRIMARY KEY,
    arb_price NUMERIC
) ON COMMIT DROP

# Step 2: Bulk insert all data (very fast - single operation)
INSERT INTO temp_arb_updates (id, arb_price) VALUES 
    (1, 0.05), (2, 0.12), (3, 0.08), ... (10000, 0.15)

# Step 3: Single UPDATE with JOIN (happens once)
UPDATE market_matching mm
SET arb_price = t.arb_price
FROM temp_arb_updates t
WHERE mm.id = t.id
```

**What happens:**
1. **Parse SQL once** - Only 3 statements total (vs 10,000)
2. **Plan query once** - PostgreSQL optimizes the JOIN operation
3. **Bulk insert** - All data inserted in one efficient operation
4. **Single UPDATE** - PostgreSQL uses optimized hash join algorithm
5. **Batch locking** - Locks acquired efficiently in batch
6. **Batch WAL writes** - More efficient logging
7. **Single network round-trip** - One result instead of 10,000

---

## Why Temporary Tables Are Fast

### 1. **Reduced SQL Parsing Overhead**
- **Old way:** Parse 10,000 UPDATE statements
- **New way:** Parse 3 statements total (CREATE, INSERT, UPDATE)
- **Savings:** ~99.97% reduction in parsing overhead

### 2. **PostgreSQL Query Planner Optimization**
PostgreSQL's query planner can optimize the JOIN operation:
```sql
UPDATE market_matching mm
SET arb_price = t.arb_price
FROM temp_arb_updates t
WHERE mm.id = t.id
```

The planner can:
- Use **hash join** (very fast for lookups)
- Use **indexes** efficiently on both tables
- **Batch process** rows instead of one-by-one
- **Parallelize** the operation (if configured)

### 3. **Efficient Bulk Insert**
```python
execute_values(cur, "INSERT INTO temp_arb_updates ... VALUES %s", data, page_size=10000)
```

This uses PostgreSQL's optimized bulk insert:
- **Minimal logging** - Less WAL overhead
- **Batch processing** - Processes multiple rows at once
- **Reduced overhead** - Single statement vs many

### 4. **Single Transaction Overhead**
- **Old way:** Each UPDATE is a separate operation in the transaction
- **New way:** One atomic operation
- **Benefit:** Less transaction overhead, better locking behavior

### 5. **Index Usage**
The temporary table can have its own index (PRIMARY KEY on id):
```sql
CREATE TEMPORARY TABLE temp_arb_updates (
    id INTEGER PRIMARY KEY,  -- This creates an index automatically!
    arb_price NUMERIC
)
```

When PostgreSQL does the JOIN:
- It can use the **hash table** built from the temp table
- Or use **index scans** on both tables
- Much faster than 10,000 individual index lookups

### 6. **Memory Efficiency**
- Temporary tables are stored in **temp buffers** (faster than regular tables)
- PostgreSQL can **cache** the entire temp table in memory
- **ON COMMIT DROP** means it's automatically cleaned up

---

## Performance Comparison

### Example: Updating 10,000 records

**Old Method (executemany):**
```
10,000 UPDATE statements × 2ms each = 20 seconds
+ Network overhead = ~25-30 seconds total
```

**New Method (temp table):**
```
CREATE TABLE: ~5ms
Bulk INSERT: ~50ms (for 10k rows)
Single UPDATE: ~100ms (with JOIN)
Total: ~155ms = 0.15 seconds
```

**Speedup: ~200x faster!**

---

## Real-World Example

Let's say you have 50,000 arbitrage opportunities to update:

### Old Approach:
```python
# 50,000 individual UPDATEs
for i in range(50000):
    cur.execute("UPDATE market_matching SET arb_price = %s WHERE id = %s", ...)
# Time: ~100-150 seconds
```

### Temp Table Approach:
```python
# 1 CREATE, 1 INSERT (bulk), 1 UPDATE (JOIN)
CREATE TEMP TABLE ...
INSERT INTO temp_table VALUES (50k rows)  # Bulk insert
UPDATE market_matching FROM temp_table ...  # Single UPDATE
# Time: ~0.5-1 second
```

**Speedup: 100-300x faster!**

---

## Why Not Just Use a Regular Table?

Temporary tables are perfect for this because:
1. **ON COMMIT DROP** - Automatically cleaned up after transaction
2. **Session-scoped** - Only visible to your connection (no conflicts)
3. **Temp storage** - Stored in faster temporary storage
4. **No logging overhead** - Less WAL logging (in some configurations)
5. **No need to clean up** - PostgreSQL handles it automatically

---

## The JOIN Operation is Key

The magic happens in this single UPDATE statement:

```sql
UPDATE market_matching mm
SET arb_price = t.arb_price
FROM temp_arb_updates t
WHERE mm.id = t.id
```

PostgreSQL's query planner sees this and thinks:
- "I need to join these two tables"
- "I can build a hash table from the temp table (it's small)"
- "I can scan market_matching and do hash lookups"
- "This is much faster than 10,000 individual WHERE id = X lookups"

The planner chooses the **most efficient join algorithm**:
- **Hash Join** - Build hash table from temp table, probe market_matching
- **Merge Join** - If both tables are sorted
- **Nested Loop** - Only if tables are very small

For bulk updates, **hash join** is typically chosen, which is extremely fast.

---

## Summary

Temporary tables help because they:
1. ✅ **Reduce SQL parsing** - 3 statements vs thousands
2. ✅ **Enable query optimization** - PostgreSQL can optimize the JOIN
3. ✅ **Use efficient algorithms** - Hash joins, batch processing
4. ✅ **Reduce overhead** - Less locking, less logging, less network
5. ✅ **Leverage indexes** - Both tables can use indexes efficiently
6. ✅ **Batch operations** - Process everything at once

**Bottom line:** Instead of doing 10,000 small operations, you do 1 big optimized operation. That's why it's 10-100x faster!

