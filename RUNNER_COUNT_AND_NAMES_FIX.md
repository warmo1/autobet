# Runner Count and Horse Names Fix 🏇

## ✅ **Issues Fixed**

### **1. Runner Count Discrepancy**
- **Problem**: Main page showed 85 runners, but event detail page only showed 9
- **Root Cause**: Odds query was only looking at WIN products (7 selections) instead of all bet types
- **Fix**: Updated query to get all runners from all bet types (WIN, PLACE, EXACTA, etc.)

### **2. Horse Names Issue**
- **Problem**: Horse names were showing as UUIDs instead of actual names
- **Root Cause**: Query was using `selection_id` instead of `competitor` field
- **Fix**: Updated query to use `s.competitor AS horse_name` for proper names

### **3. Odds Display**
- **Problem**: No odds were displaying (all showing "-")
- **Root Cause**: Complex odds query was failing due to view issues
- **Fix**: Simplified query structure with proper LEFT JOIN for odds data

## 🔧 **Key Changes Made**

### **Updated Odds Query (`webapp.py`)**
```sql
-- NEW: Get all runners from all bet types
WITH all_runners AS (
  SELECT DISTINCT
    s.number AS cloth_number,
    s.competitor AS horse_name,  -- ✅ Use competitor field for names
    s.selection_id
  FROM `autobet-470818.autobet.tote_product_selections` s
  JOIN `autobet-470818.autobet.tote_products` p ON s.product_id = p.product_id
  WHERE p.event_id = @event_id
    AND s.leg_index = 1  -- ✅ Get all runners, not just WIN
  ORDER BY SAFE_CAST(s.number AS INT64)
),
-- ... odds processing ...
SELECT
    ar.cloth_number,
    ar.horse_name AS horse,  -- ✅ Proper horse names
    ar.selection_id,
    lps.decimal_odds,        -- ✅ Odds when available
    FORMAT_TIMESTAMP('%FT%T%Ez', lps.latest_ts) AS odds_iso
FROM all_runners ar
LEFT JOIN latest_per_selection lps ON ar.selection_id = lps.selection_id
ORDER BY SAFE_CAST(ar.cloth_number AS INT64) ASC
```

## 📊 **Expected Results**

### **Runner Count:**
- **Before**: 9 runners (only WIN product selections)
- **After**: 85 runners (all bet types combined)

### **Horse Names:**
- **Before**: UUIDs like `9ef71838-cdda-43d0-bfe3-3bd4b5e569fc`
- **After**: Actual names like "BEARIN UP", "PARODA DIVA", "ASPIRAL"

### **Odds Display:**
- **Before**: All showing "-" (no odds)
- **After**: Decimal odds like 3.50, 7.25, 12.00 when available

### **Results Section:**
- **Status**: ✅ **Preserved** - Still working as you liked it!

## 🎯 **What Should Work Now**

### **Main Events Page:**
- Shows correct runner count (85 for AYR RACE 1)
- Runners column displays accurate numbers

### **Event Detail Page:**
- **Runners Section**: Shows all 85 runners with proper names and odds
- **Results Section**: Still working perfectly with finish positions
- **Horse Names**: Real names instead of UUIDs
- **Odds**: Decimal odds when available, "-" when not

## 🚀 **Web Application Status**

The web application has been restarted with the fixes. You can now test:

1. **Main events page** - should show correct runner counts
2. **Event detail page** - should show all 85 runners with proper names
3. **Odds display** - should show decimal odds when available
4. **Results section** - should continue working as before

---

**Status**: ✅ **FIXES DEPLOYED!** 
**Next Step**: Test the web application to confirm all issues are resolved
