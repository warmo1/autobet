# Correct Runner Count Fix 🏇

## ✅ **Understanding the Issue**

You were absolutely right! I misunderstood the data structure:

- **85 "runners"** = All bet combinations (EXACTA, TRIFECTA, SUPERFECTA, etc.) using the same 9 horses
- **9 actual horses** = The real runners in the race (WIN bet selections only)
- **Other bet types** = Combinations of these 9 horses, not additional runners

## 🔧 **Fixes Applied**

### **1. Updated Runner Count View**
```sql
-- OLD: Counted all combinations (85)
SELECT e.event_id, COALESCE(MAX(pc.n_competitors), 0) AS n_runners
FROM tote_events e
LEFT JOIN tote_products p ON e.event_id = p.event_id
LEFT JOIN vw_product_competitor_counts pc ON p.product_id = pc.product_id

-- NEW: Count only actual horses from WIN bets (9)
SELECT p.event_id, COUNT(DISTINCT s.selection_id) AS n_runners
FROM tote_products p
JOIN tote_product_selections s ON p.product_id = s.product_id
WHERE UPPER(p.bet_type) = 'WIN' AND s.leg_index = 1
GROUP BY p.event_id
```

### **2. Updated Odds Query**
```sql
-- OLD: Got all runners from all bet types
WITH all_runners AS (
  SELECT DISTINCT s.number, s.competitor, s.selection_id
  FROM tote_product_selections s
  JOIN tote_products p ON s.product_id = p.product_id
  WHERE p.event_id = @event_id AND s.leg_index = 1
)

-- NEW: Get only WIN bet horses
WITH win_runners AS (
  SELECT DISTINCT s.number, s.competitor, s.selection_id
  FROM tote_product_selections s
  JOIN tote_products p ON s.product_id = p.product_id
  WHERE p.event_id = @event_id 
    AND UPPER(p.bet_type) = 'WIN'  -- ✅ Only WIN bets
    AND s.leg_index = 1
)
```

## 📊 **Expected Results**

### **Main Events Page:**
- **Before**: 85 runners (all combinations)
- **After**: 9 runners (actual horses only)

### **Event Detail Page:**
- **Runners Section**: 9 horses with proper names and odds
- **Pools Section**: All bet types (EXACTA, TRIFECTA, etc.) with pool info
- **Results Section**: 9 horses with finish positions (unchanged)

## 🎯 **What You Should See Now**

### **AYR RACE 1:**
- **Main page**: Shows "9 runners" (not 85)
- **Event detail**: Shows 9 horses with names like "BEARIN UP", "PARODA DIVA"
- **Odds**: Decimal odds for each horse when available
- **Pools**: EXACTA, TRIFECTA, SUPERFECTA, etc. with pool amounts
- **Results**: 9 horses with finish positions (1st, 2nd, 3rd, etc.)

## 🚀 **Status**

- ✅ **Runner count logic fixed** - Now counts actual horses, not combinations
- ✅ **Odds query fixed** - Only shows WIN bet horses with proper names
- ✅ **Web application restarted** - Changes are live
- 🔄 **Main page count** - Should now show 9 instead of 85

The web application is running at http://localhost:8010 with the corrected logic!

---

**Key Insight**: The 85 count was from all bet combinations using the same 9 horses, not 85 different horses. Now it correctly shows only the 9 actual horses in the race. 🏇
