-- Routine type: PROCEDURE
CREATE OR REPLACE PROCEDURE `autobet-470818.autobet.sp_set_tote_params`(
  in_model_id STRING,
  in_ts_ms INT64,
  in_t FLOAT64,
  in_f FLOAT64,
  in_stake FLOAT64,
  in_R FLOAT64,
  in_default_top_n INT64,
  in_target_coverage FLOAT64
)
AS
BEGIN
  DECLARE chosen_ts_ms INT64 DEFAULT in_ts_ms;
  IF chosen_ts_ms IS NULL THEN
    SET chosen_ts_ms = (
      SELECT MAX(ts_ms)
      FROM `autobet-470818.autobet.predictions`
      WHERE model_id = in_model_id
    );
  END IF;

  INSERT INTO `autobet-470818.autobet.tote_params`
    (t, f, stake_per_line, R, model_id, ts_ms, default_top_n, target_coverage)
  VALUES (in_t, in_f, in_stake, in_R, in_model_id, chosen_ts_ms, in_default_top_n, in_target_coverage);
END
