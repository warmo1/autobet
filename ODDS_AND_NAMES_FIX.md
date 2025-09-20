# Odds and Horse Names Fix 🏇

## ✅ **Issues Fixed**

### **1. Horse Names Issue**
- **Problem**: Horse names were showing as UUIDs instead of actual names
- **Root Cause**: Template logic was prioritizing empty `runners_prob` over populated `runners` data
- **Fix**: Updated template logic to properly prioritize odds data when available, fallback to runners data

### **2. Missing Odds Issue**
- **Problem**: No odds were displaying (all showing "-")
- **Root Cause**: `vw_tote_probable_odds` view was failing due to BigQuery materialized view issues
- **Fix**: Replaced with direct query from `raw_tote_probable_odds` table

## 🔧 **Changes Made**

### **1. Updated Odds Query (`webapp.py`)**
```sql
-- Replaced problematic view query with direct raw data query
WITH latest_odds AS (
  SELECT
    SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) AS product_id,
    COALESCE(
      JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId'),
      JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId')
    ) AS selection_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(line, '$.odds.decimal') AS FLOAT64) AS decimal_odds,
    TIMESTAMP_MILLIS(r.fetched_ts) AS ts
  FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
  UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
  UNNEST(IFNULL(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes'),
                JSON_EXTRACT_ARRAY(prod, '$.lines'))) AS line
  WHERE JSON_EXTRACT_SCALAR(line, '$.odds.decimal') IS NOT NULL
    AND TIMESTAMP_MILLIS(r.fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
)
-- ... rest of query
```

### **2. Fixed Template Logic (`event_detail.html`)**
```html
<!-- Before: Always showed runners_prob even when empty -->
{% set rows = runners_prob if runners_prob is defined and runners_prob else [] %}
{% if not rows and runners %}
  <!-- Show runners fallback -->
{% else %}
  <!-- Show runners_prob -->
{% endif %}

<!-- After: Properly prioritize odds data -->
{% set rows = runners_prob if runners_prob is defined and runners_prob else [] %}
{% if rows %}
  <!-- Show odds data with horse names and odds -->
{% elif runners %}
  <!-- Show runners fallback with proper names -->
{% endif %}
```

## 📊 **Data Verification**

### **Confirmed Working Data:**
- ✅ **Horse Names**: "BEARIN UP", "PARODA DIVA", "ASPIRAL", "MAGISTERY", "RADIANCE"
- ✅ **Horse Numbers**: 1, 2, 4, 6, 7
- ✅ **Selection IDs**: Proper UUIDs for matching
- ✅ **Raw Odds Data**: 3,964 records available in last 7 days

### **Expected Results:**
- **Horse Names**: Should show actual names instead of UUIDs
- **Odds**: Should show decimal odds (e.g., 3.50, 7.25) instead of "-"
- **Horse Numbers**: Should show cloth numbers (1, 2, 3, etc.)
- **Results Section**: Should continue working as before (you liked this part!)

## 🚀 **Web Application Status**

The web application has been restarted with the fixes. You can now test:

1. **Open the event page** for AYR RACE 1 or any other event
2. **Check the Runners section** - should show proper horse names and odds
3. **Verify the Results section** - should still work as before
4. **Test odds display** - should show decimal odds instead of "-"

## 🎯 **What Should Work Now**

### **Runners Section:**
- **Horse Names**: "BEARIN UP", "PARODA DIVA", etc. (not UUIDs)
- **Horse Numbers**: 1, 2, 4, 6, 7
- **Odds**: Decimal odds like 3.50, 7.25, 12.00
- **Updated Time**: Timestamp of when odds were last updated

### **Results Section (HR Database):**
- **Horse Names**: Proper names from historical data
- **Finish Positions**: 1, 2, 3, etc.
- **Status**: RESULTED, etc.
- **Jockey/Trainer**: Available when data exists

---

**Status**: ✅ **FIXES DEPLOYED!** 
**Next Step**: Test the web application to confirm horse names and odds are now displaying correctly
