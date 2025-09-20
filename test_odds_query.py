#!/usr/bin/env python3
"""Test the updated odds query"""

from sports.bq import get_bq_sink

def main():
    sink = get_bq_sink()
    event_id = "HORSERACING-AYR-GB-2025-09-19-1228"
    
    print("=== Testing Updated Odds Query ===")
    
    # Test the exact query from webapp.py
    try:
        result = sink.query(f"""
        WITH win_runners AS (
          SELECT DISTINCT
            s.number AS cloth_number,
            s.competitor AS horse_name,
            s.selection_id
          FROM `autobet-470818.autobet.tote_product_selections` s
          JOIN `autobet-470818.autobet.tote_products` p ON s.product_id = p.product_id
          WHERE p.event_id = '{event_id}'
            AND UPPER(p.bet_type) = 'WIN'
            AND s.leg_index = 1
          ORDER BY SAFE_CAST(s.number AS INT64)
        ),
        latest_odds AS (
          SELECT
            SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) AS product_id,
            COALESCE(
              JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId'),
              JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId')
            ) AS selection_id,
            SAFE_CAST(JSON_EXTRACT_SCALAR(line, '$.odds[0].decimal') AS FLOAT64) AS decimal_odds,
            TIMESTAMP_MILLIS(r.fetched_ts) AS ts
          FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
          UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
          UNNEST(IFNULL(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes'),
                        JSON_EXTRACT_ARRAY(prod, '$.lines'))) AS line
          WHERE JSON_EXTRACT_SCALAR(line, '$.odds[0].decimal') IS NOT NULL
            AND TIMESTAMP_MILLIS(r.fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ),
        latest_per_selection AS (
          SELECT
            product_id,
            selection_id,
            (ARRAY_AGG(STRUCT(decimal_odds, ts) ORDER BY ts DESC LIMIT 1))[OFFSET(0)].decimal_odds AS decimal_odds,
            MAX(ts) AS latest_ts
          FROM latest_odds
          WHERE selection_id IS NOT NULL
            AND decimal_odds IS NOT NULL 
            AND decimal_odds > 0
            AND product_id IS NOT NULL
          GROUP BY product_id, selection_id
        )
        SELECT
            wr.cloth_number,
            COALESCE(wr.horse_name, wr.selection_id) AS horse,
            wr.selection_id,
            lps.decimal_odds,
            FORMAT_TIMESTAMP('%FT%T%Ez', lps.latest_ts) AS odds_iso
        FROM win_runners wr
        LEFT JOIN latest_per_selection lps ON wr.selection_id = lps.selection_id
        ORDER BY SAFE_CAST(wr.cloth_number AS INT64) ASC
        """)
        
        print("Query Results:")
        count = 0
        for row in result:
            count += 1
            print(f"  {row.cloth_number}: {row.horse} - {row.decimal_odds} ({row.odds_iso})")
        
        if count == 0:
            print("  No results found!")
        else:
            print(f"  Found {count} results")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
