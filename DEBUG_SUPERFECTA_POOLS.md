# Superfecta Pool Debug Guide

This guide provides step-by-step instructions for diagnosing and fixing Superfecta pool data issues in the autobet application.

## 🚨 Common Issues

- **Zero pool values** showing in web app despite API having real data
- **GraphQL schema mismatches** between our queries and Tote API
- **Database query parameter errors** in diagnostic scripts
- **Missing environment variables** for API access

## 🔧 Prerequisites

### 1. Environment Setup

Create a `.env` file in the project root with required variables:

```bash
# Required for Tote API access
TOTE_API_KEY="your_actual_api_key_here"
TOTE_GRAPHQL_URL="https://hub.production.racing.tote.co.uk/partner/gateway/graphql/"

# Optional for BigQuery access (if testing database queries)
GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
# OR
GCP_SERVICE_ACCOUNT_JSON='{"type": "service_account", ...}'
```

### 2. Python Environment

Set up a virtual environment and install dependencies:

```bash
# Create virtual environment
python3 -m venv test_env
source test_env/bin/activate  # On Windows: test_env\Scripts\activate

# Install required packages
pip install requests python-dotenv google-cloud-bigquery
```

## 🔍 Diagnostic Tools

### 1. Test API Connection

Test basic API connectivity and GraphQL query structure:

```bash
python3 scripts/test_tote_api.py
```

**Expected Output:**
```
✅ API Connection Successful
📊 Product: [Product Name]
  Bet Type: SUPERFECTA
  Status: [OPEN/CLOSED]
  Gross: [non-zero value]
  Net: [non-zero value]
  Rollover: [value]
  Takeout: [percentage]%
```

**If you get errors:**
- `401 Unauthorized`: Check your `TOTE_API_KEY`
- `GraphQL errors`: Schema mismatch - see GraphQL Query Fixes section

### 2. Comprehensive Pool Diagnosis

Run the full diagnostic script to compare API vs Database data:

```bash
python scripts/diagnose_superfecta_pools.py --product-id b38870ed-1647-436e-8a8b-b4c0fbfc8b1a
```

**Expected Output:**
```
📡 API Data:
  Bet Type: SUPERFECTA
  Status: CLOSED
  Gross: 67.29
  Net: 47.11
  Rollover: 56.01
  Takeout: 30.0%

🗄️ Database Pool Snapshots:
  Product ID: b38870ed-1647-436e-8a8b-b4c0fbfc8b1a
  Gross: 67.29
  Net: 47.11
  Rollover: 56.01
  Timestamp: [timestamp]

📊 Database Product Data:
  Bet Type: SUPERFECTA
  Status: Closed
  Gross: 67.29
  Net: 47.11
  Rollover: 56.01
  Event: HORSERACING-NEWCASTLE-GB-2025-09-19-1815

🔍 Analysis:
✅ API shows non-zero pool values
✅ Database shows non-zero pool values
✅ Data is synchronized
```

### 3. Manual Pool Import

Force import of specific Superfecta product data:

```bash
# Import specific product
python scripts/manual_superfecta_import.py --product-id b38870ed-1647-436e-8a8b-b4c0fbfc8b1a

# Import all OPEN Superfecta products for a date
python scripts/manual_superfecta_import.py --date 2025-01-15
```

**Expected Output:**
```
Importing Superfecta product: b38870ed-1647-436e-8a8b-b4c0fbfc8b1a
Ingesting products for date=None, status=None, bet_types=['SUPERFECTA'], product_ids=['b38870ed-1647-436e-8a8b-b4c0fbfc8b1a']
Stored raw probable odds payload.
Inserting 1 rows into tote_products
Inserting 10 rows into tote_product_selections
Inserting 1 rows into tote_events
Inserting 1 rows into tote_bet_rules
Successfully ingested product data.
✅ Successfully imported 1 Superfecta product(s)
```

### 4. Show Product Pool Data

Display pool data for a specific product:

```bash
python scripts/show_product_pool.py --id b38870ed-1647-436e-8a8b-b4c0fbfc8b1a
```

## 🛠️ Common Fixes

### 1. GraphQL Query Structure Issues

**Problem:** `The field 'grossAmounts' does not exist on the type 'BettingProductPoolTotal'`

**Root Cause:** Our queries were using array format (`grossAmounts`) but Tote API uses single object format (`grossAmount`)

**Solution:** Ensure all GraphQL queries use the correct structure:

```graphql
# ✅ CORRECT - Single object format
pool {
  total {
    grossAmount { decimalAmount }
    netAmount { decimalAmount }
  }
  carryIn {
    grossAmount { decimalAmount }
    netAmount { decimalAmount }
  }
  takeout {
    percentage
    amount { decimalAmount }
  }
}

# ❌ WRONG - Array format (causes API errors)
pool {
  total {
    grossAmounts { decimalAmount }  # This field doesn't exist
    netAmounts { decimalAmount }    # This field doesn't exist
  }
}
```

**Files to check:**
- `sports/ingest/tote_products.py` - PRODUCTS_QUERY and PRODUCT_BY_ID_QUERY
- `scripts/diagnose_superfecta_pools.py` - GraphQL query
- `scripts/test_tote_api.py` - GraphQL query
- `scripts/show_product_pool.py` - GraphQL query

### 2. Data Structure Changes

**Problem:** Data parsing errors due to API structure changes

**Root Cause:** Tote API now wraps product data under `node.type` instead of directly on the node

**Solution:** Update data parsing to use the new structure:

```python
# ✅ CORRECT - New structure
product = data.get("product", {})
product_type = product.get("type", {})
pool = product_type.get("pool", {})

# ❌ WRONG - Old structure
product = data.get("product", {})
pool = product.get("pool", {})  # This will be None now
```

### 3. Database Query Parameter Errors

**Problem:** `BigQuerySink.query() takes 2 positional arguments but 3 were given`

**Root Cause:** BigQuery expects parameters via `query_parameters` in `QueryJobConfig`, not as positional arguments

**Solution:** Use proper BigQuery parameterized queries:

```python
# ✅ CORRECT - BigQuery parameterized query
from google.cloud import bigquery

query = """
SELECT * FROM table WHERE product_id = @product_id
"""

job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("product_id", "STRING", product_id)
    ]
)
rows = list(db.query(query, job_config=job_config))

# ❌ WRONG - Positional arguments
rows = list(db.query(query, product_id))  # This causes errors
```

### 4. Competitor Details Structure

**Problem:** Cloth numbers not displaying correctly

**Root Cause:** API structure changed from `eventCompetitor` to `competitor.details`

**Solution:** Update competitor parsing:

```python
# ✅ CORRECT - New structure
competitor = sel.get("competitor", {})
details = competitor.get("details", {})
cloth_number = details.get("clothNumber")

# ❌ WRONG - Old structure
event_comp = sel.get("eventCompetitor", {})
cloth_number = event_comp.get("clothNumber")
```

## 🔄 WebSocket Subscription Fixes

### 1. WebSocket Message Format

**Problem:** WebSocket subscriptions not receiving updates

**Root Cause:** Tote API uses standard WebSocket with JSON messages, not GraphQL WebSocket protocol

**Solution:** Update WebSocket message parsing in `sports/providers/tote_subscriptions.py`:

```python
# ✅ CORRECT - Look for MessageType in JSON
def _on_message(self, ws, message):
    try:
        data = json.loads(message)
        message_type = data.get("MessageType")
        
        if message_type == "PoolTotalChanged":
            # Handle pool total updates
        elif message_type == "EventStatusChanged":
            # Handle event status updates
        # ... etc

# ❌ WRONG - GraphQL WebSocket protocol
def _on_message(self, ws, message):
    # Don't expect GraphQL subscription format
```

### 2. Pool Data Parsing

**Problem:** Pool totals not updating in real-time

**Root Cause:** Incorrect parsing of WebSocket pool data

**Solution:** Parse pool data according to Tote API documentation:

```python
# ✅ CORRECT - Parse pool data from WebSocket
if message_type == "PoolTotalChanged":
    pool_data = data.get("PoolTotalChanged", {})
    product_id = pool_data.get("ProductId")
    gross_amount = pool_data.get("GrossAmount", {}).get("DecimalAmount")
    net_amount = pool_data.get("NetAmount", {}).get("DecimalAmount")
    
    # Update UI and database
    self._update_pool_totals(product_id, gross_amount, net_amount)
```

## 🧪 Testing Checklist

### 1. API Connection Test
- [ ] `test_tote_api.py` runs without errors
- [ ] Returns non-zero pool values
- [ ] Shows correct product information

### 2. Database Integration Test
- [ ] `diagnose_superfecta_pools.py` shows database data
- [ ] Pool values match between API and database
- [ ] No BigQuery parameter errors

### 3. Manual Import Test
- [ ] `manual_superfecta_import.py` successfully imports data
- [ ] BigQuery tables are updated
- [ ] Web app shows updated pool values

### 4. Web App Test
- [ ] Pool totals display correctly on `/tote-pools` page
- [ ] Real-time updates work via WebSocket
- [ ] Event detail pages show correct pool data

## 🚨 Emergency Recovery

If Superfecta pools are completely broken:

1. **Check API connectivity:**
   ```bash
   python3 scripts/test_tote_api.py
   ```

2. **Force re-import all Superfecta data:**
   ```bash
   python scripts/manual_superfecta_import.py --date $(date +%Y-%m-%d)
   ```

3. **Verify database has data:**
   ```bash
   python scripts/diagnose_superfecta_pools.py --product-id [any-superfecta-id]
   ```

4. **Restart web application** to clear any cached data

## 📞 Support

If issues persist after following this guide:

1. Check the application logs for specific error messages
2. Verify all environment variables are set correctly
3. Ensure BigQuery credentials have proper permissions
4. Test with a known working Superfecta product ID

## 📝 Notes

- The Tote API structure may change over time
- Always test with a small number of products before bulk operations
- Keep backup of working GraphQL queries for quick recovery
- Monitor BigQuery costs when running diagnostic queries frequently
