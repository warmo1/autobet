"""Bootstrap SQL objects needed for the Superfecta ML workflow.

This script creates the training/live feature views in the main dataset and
ensures the predictions table lives in the companion `autobet_model` dataset.

It assumes you have credentials configured for the `autobet-470818` project.
Run via: `python scripts/setup_superfecta_ml.py`
"""

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "autobet-470818"
PRIMARY_DATASET = "autobet"
MODEL_DATASET = "autobet_model"


FEATURE_COLUMN_STATEMENTS = [
    """
    ALTER TABLE `autobet-470818.autobet.features_runner_event`
    ADD COLUMN IF NOT EXISTS recent_runs INT64
    """,
    """
    ALTER TABLE `autobet-470818.autobet.features_runner_event`
    ADD COLUMN IF NOT EXISTS avg_finish FLOAT64
    """,
    """
    ALTER TABLE `autobet-470818.autobet.features_runner_event`
    ADD COLUMN IF NOT EXISTS wins_last5 INT64
    """,
    """
    ALTER TABLE `autobet-470818.autobet.features_runner_event`
    ADD COLUMN IF NOT EXISTS places_last5 INT64
    """,
    """
    ALTER TABLE `autobet-470818.autobet.features_runner_event`
    ADD COLUMN IF NOT EXISTS days_since_last_run INT64
    """,
    """
    ALTER TABLE `autobet-470818.autobet.features_runner_event`
    ADD COLUMN IF NOT EXISTS weight_kg FLOAT64
    """,
    """
    ALTER TABLE `autobet-470818.autobet.features_runner_event`
    ADD COLUMN IF NOT EXISTS weight_lbs FLOAT64
    """,
]


VIEW_STATEMENTS = [
    """
    CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_superfecta_training_base` AS
    SELECT
      p.event_id,
      p.product_id,
      DATE(SUBSTR(COALESCE(te.start_iso, p.start_iso), 1, 10)) AS event_date,
      COALESCE(te.venue, p.venue) AS venue,
      te.country,
      p.total_net,
      rc.going,
      rc.weather_temp_c,
      rc.weather_wind_kph,
      rc.weather_precip_mm,
      r.horse_id,
      r.cloth_number,
      r.finish_pos,
      r.status
    FROM `autobet-470818.autobet.tote_products` p
    JOIN `autobet-470818.autobet.hr_horse_runs` r ON r.event_id = p.event_id
    LEFT JOIN `autobet-470818.autobet.race_conditions` rc ON rc.event_id = p.event_id
    LEFT JOIN `autobet-470818.autobet.tote_events` te ON te.event_id = p.event_id
    WHERE UPPER(p.bet_type) = 'SUPERFECTA'
      AND r.finish_pos IS NOT NULL
      AND r.horse_id IS NOT NULL;
    """,
    """
    CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_superfecta_training` AS
    SELECT * FROM `autobet-470818.autobet.vw_superfecta_training_base`;
    """,
    """
    CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_superfecta_runner_training_features` AS
    SELECT
      base.event_id,
      base.product_id,
      base.event_date,
      base.venue,
      base.country,
      base.total_net,
      base.going,
      base.weather_temp_c,
      base.weather_wind_kph,
      base.weather_precip_mm,
      base.horse_id,
      base.cloth_number,
      base.finish_pos,
      base.status,
      feat.recent_runs,
      feat.avg_finish,
      feat.wins_last5,
      feat.places_last5,
      feat.days_since_last_run,
      feat.weight_kg,
      feat.weight_lbs
    FROM `autobet-470818.autobet.vw_superfecta_training_base` base
    LEFT JOIN `autobet-470818.autobet.features_runner_event` feat
      ON base.event_id = feat.event_id AND base.horse_id = feat.horse_id;
    """,
    """
    CREATE OR REPLACE VIEW `autobet-470818.autobet.vw_superfecta_runner_live_features` AS
    SELECT
      p.product_id,
      feat.event_id,
      DATE(SUBSTR(COALESCE(te.start_iso, p.start_iso), 1, 10)) AS event_date,
      p.event_name,
      COALESCE(te.venue, p.venue) AS venue,
      te.country,
      p.start_iso,
      COALESCE(p.status, te.status) AS status,
      p.total_net,
      COALESCE(rc.going, feat.going) AS going,
      COALESCE(rc.weather_temp_c, feat.weather_temp_c) AS weather_temp_c,
      COALESCE(rc.weather_wind_kph, feat.weather_wind_kph) AS weather_wind_kph,
      COALESCE(rc.weather_precip_mm, feat.weather_precip_mm) AS weather_precip_mm,
      feat.horse_id,
      feat.cloth_number,
      feat.recent_runs,
      feat.avg_finish,
      feat.wins_last5,
      feat.places_last5,
      feat.days_since_last_run,
      feat.weight_kg,
      feat.weight_lbs
    FROM `autobet-470818.autobet.tote_products` p
    JOIN `autobet-470818.autobet.features_runner_event` feat
      ON feat.event_id = p.event_id AND feat.horse_id IS NOT NULL
    LEFT JOIN `autobet-470818.autobet.race_conditions` rc ON rc.event_id = p.event_id
    LEFT JOIN `autobet-470818.autobet.tote_events` te ON te.event_id = p.event_id
    WHERE UPPER(p.bet_type) = 'SUPERFECTA';
    """,
]


PREDICTIONS_TABLE_STMT = """
CREATE TABLE IF NOT EXISTS `autobet-470818.autobet_model.superfecta_runner_predictions` (
  model_id STRING,
  model_version STRING,
  scored_at TIMESTAMP,
  product_id STRING,
  event_id STRING,
  horse_id STRING,
  runner_key STRING,
  p_place1 FLOAT64,
  p_place2 FLOAT64,
  p_place3 FLOAT64,
  p_place4 FLOAT64,
  features_json STRING
)
PARTITION BY DATE(scored_at)
CLUSTER BY event_id, product_id;
"""


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset_ref.location = "EU"
        client.create_dataset(dataset_ref)
        print(f"Created dataset {PROJECT_ID}.{dataset_id}")


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    ensure_dataset(client, PRIMARY_DATASET)
    ensure_dataset(client, MODEL_DATASET)

    for statement in FEATURE_COLUMN_STATEMENTS:
        print("Ensuring feature column exists...")
        try:
            client.query(statement).result()
        except NotFound:
            print("features_runner_event table not found; skipping optional columns.")
            break

    for statement in VIEW_STATEMENTS:
        print("Executing SQL for view creation...")
        client.query(statement).result()

    print("Ensuring predictions table exists...")
    client.query(PREDICTIONS_TABLE_STMT).result()
    print("Superfecta ML objects are ready.")


if __name__ == "__main__":
    main()
