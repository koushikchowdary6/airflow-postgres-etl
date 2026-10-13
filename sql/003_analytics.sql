-- Rebuild the aggregate model from the fact table.
--
-- Rebuilt in full rather than incrementally: the output is ~100 rows, so a
-- full rebuild is cheaper than the bookkeeping an incremental refresh needs,
-- and it cannot drift from the fact table. TRUNCATE + INSERT runs inside the
-- caller's transaction, so readers never see an empty table.

TRUNCATE TABLE analytics.congress_age_summary;

INSERT INTO analytics.congress_age_summary (
      congress
    , chamber
    , member_count
    , mean_age_years
    , median_age_years
    , min_age_years
    , max_age_years
    , pct_aged_65_plus
    , median_change_vs_prev
    , oldest_member
)
WITH per_group AS (
    SELECT
          congress
        , chamber
        , COUNT(*)                                     AS member_count
        , AVG(age_years)                               AS mean_age_years
        -- percentile_cont operates on double precision; cast back to numeric
        -- so the stored column keeps a fixed scale.
        , PERCENTILE_CONT(0.5) WITHIN GROUP (
              ORDER BY age_years::double precision
          )::numeric                                   AS median_age_years
        , MIN(age_years)                               AS min_age_years
        , MAX(age_years)                               AS max_age_years
        , 100.0 * COUNT(*) FILTER (WHERE age_years >= 65) / COUNT(*) AS pct_aged_65_plus
        -- Oldest member in the group, without a self-join.
        , (ARRAY_AGG(bioname ORDER BY age_years DESC))[1]            AS oldest_member
    FROM analytics.member_term
    GROUP BY congress, chamber
)
SELECT
      congress
    , chamber
    , member_count
    , ROUND(mean_age_years, 4)
    , ROUND(median_age_years, 4)
    , ROUND(min_age_years, 4)
    , ROUND(max_age_years, 4)
    , ROUND(pct_aged_65_plus, 2)
    -- Change in median age against the previous Congress in the same chamber.
    -- NULL for each chamber's first Congress, which is correct rather than 0.
    , ROUND(
          median_age_years - LAG(median_age_years) OVER (
              PARTITION BY chamber ORDER BY congress
          )
      , 4)
    , oldest_member
FROM per_group;
