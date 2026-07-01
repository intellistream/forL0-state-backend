-- -------------------------------------------------------------------------------------------------
-- Query 5 steady-state variant: Hot Item Window Counts
-- -------------------------------------------------------------------------------------------------
-- Non-contract TPS/backpressure variant for ForL0 evaluation.
-- The original q5 emits only the global maximum per window; in unbounded TPS mode this can produce
-- long zero-output intervals.  This variant keeps the q5 sliding-window state pressure and emits
-- every auction's window count so sink-side TPS is a stable end-to-end metric.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q5 (
  auction  BIGINT,
  num  BIGINT,
  starttime TIMESTAMP(3),
  endtime TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q5
SELECT
  auction,
  count(*) AS num,
  window_start AS starttime,
  window_end AS endtime
FROM TABLE(
  HOP(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
GROUP BY auction, window_start, window_end;
