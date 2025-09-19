# Runners Column Implementation

This document describes the implementation of the runners column on the main events page and events listing page.

## 🎯 Overview

Added a "Runners" column to display the number of runners/competitors for each race event on:
- Main page (index.html) - "Now / Next Races" section
- Events page (tote_events.html) - Full events listing

## 🏗️ Implementation Details

### 1. Database Views Created

**vw_product_competitor_counts**
```sql
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_product_competitor_counts` AS
SELECT 
  product_id,
  COUNT(1) AS n_competitors
FROM `autobet-470818.autobet.tote_product_selections`
GROUP BY product_id;
```

**vw_event_runner_counts**
```sql
CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_event_runner_counts` AS
SELECT 
  e.event_id,
  COALESCE(MAX(pc.n_competitors), 0) AS n_runners
FROM `autobet-470818.autobet.tote_events` e
LEFT JOIN `autobet-470818.autobet.tote_products` p ON e.event_id = p.event_id
LEFT JOIN `autobet-470818.autobet.vw_product_competitor_counts` pc ON p.product_id = pc.product_id
GROUP BY e.event_id;
```

### 2. Backend Changes

**Main Page Query (webapp.py - index function)**
- Updated to include `n_runners` field
- Uses optimized `vw_event_runner_counts` view
- Shows runner count for events in next 24 hours

**Events Page Query (webapp.py - tote_events_page function)**
- Updated to include `n_runners` field
- Uses optimized `vw_event_runner_counts` view
- Shows runner count for all filtered events

### 3. Frontend Changes

**Main Page Template (index.html)**
- Added "Runners" column header
- Added runners count display with center alignment
- Shows `{{ e.n_runners or 0 }}` for each event

**Events Page Template (tote_events.html)**
- Added "Runners" column header
- Added runners count display with center alignment
- Updated colspan for "No events found" message
- Shows `{{ e.n_runners or 0 }}` for each event

## 📊 Data Flow

1. **Data Source**: `tote_product_selections` table contains individual runner selections
2. **Aggregation**: `vw_product_competitor_counts` counts runners per product
3. **Event Level**: `vw_event_runner_counts` aggregates to event level
4. **Display**: Queries join with events to show runner counts

## 🚀 Deployment

### Deploy Views
```bash
# Deploy the new views
bq query --use_legacy_sql=false --project_id=autobet-470818 < performance_optimization.sql
```

### Test Implementation
```bash
# Test the runners column functionality
python scripts/test_runners_column.py
```

### Deploy Full System
```bash
# Deploy all optimizations including runners column
./scripts/deploy_performance_optimizations.sh
```

## 🧪 Testing

The implementation includes a test script that verifies:
- Views are created correctly
- Queries return runner counts
- Data is properly formatted

Run the test:
```bash
python scripts/test_runners_column.py
```

## 📈 Performance Considerations

- **Optimized Queries**: Uses pre-computed views instead of complex joins
- **Efficient Aggregation**: Runner counts are calculated once and reused
- **Minimal Impact**: Only adds one column to existing queries
- **Cached Results**: Leverages existing caching mechanisms

## 🎨 UI/UX Features

- **Center Aligned**: Runner counts are center-aligned for better readability
- **Fallback Display**: Shows "0" when no runners data is available
- **Consistent Styling**: Matches existing table styling
- **Responsive**: Works with existing responsive table design

## 🔧 Configuration

No additional configuration required. The runners column will automatically:
- Show runner counts for all events
- Display "0" for events without runner data
- Update in real-time as new data is ingested

## 📝 Notes

- Runner counts are based on `tote_product_selections` data
- Events without products will show 0 runners
- The count represents the maximum number of runners across all products for an event
- Data is refreshed based on existing cache refresh intervals

---

**Status**: ✅ Implemented and Ready for Deployment
**Files Modified**: 
- `sports/webapp.py` (2 functions)
- `sports/templates/index.html`
- `sports/templates/tote_events.html`
- `performance_optimization.sql` (added views)
- `scripts/test_runners_column.py` (new test script)
