-- Data-quality checks.
--
-- Returns one row per check: (check_name, status, observed, detail).
-- `observed` is the number of offending rows, so a failure can be turned into
-- a list of bad rows by re-running the same predicate as a SELECT.
--
-- Some of these duplicate a database constraint on purpose. A constraint stops
-- bad data getting in; a check proves it did not get in, and would catch a
-- constraint that was dropped or added NOT VALID.

WITH duplicate_natural_key AS (
    SELECT congress, bioguide_id
    FROM analytics.member_term
    GROUP BY congress, bioguide_id
    HAVING COUNT(*) > 1
)
, staged_not_loaded AS (
    SELECT s.congress, s.bioguide_id
    FROM staging.member_term_stage AS s
    LEFT JOIN analytics.member_term AS f
           ON f.congress = s.congress
          AND f.bioguide_id = s.bioguide_id
    WHERE f.bioguide_id IS NULL
)
, staged_value_mismatch AS (
    -- Every staged row must be present in the fact table with the staged
    -- values. Catches an upsert that matched but failed to apply a change.
    SELECT s.congress, s.bioguide_id
    FROM staging.member_term_stage AS s
    JOIN analytics.member_term AS f
      ON f.congress = s.congress
     AND f.bioguide_id = s.bioguide_id
    WHERE (f.age_years, f.chamber, f.party_code, f.state_abbrev, f.start_date)
          IS DISTINCT FROM
          (s.age_years, s.chamber, s.party_code, s.state_abbrev, s.start_date)
)
, summary_vs_fact AS (
    -- Cross-model check: the aggregate's member counts must reconcile with the
    -- fact table group by group. Catches a stale or partially built model.
    SELECT g.congress, g.chamber
    FROM (
        SELECT congress, chamber, COUNT(*) AS fact_count
        FROM analytics.member_term
        GROUP BY congress, chamber
    ) AS g
    FULL OUTER JOIN analytics.congress_age_summary AS s
                 ON s.congress = g.congress AND s.chamber = g.chamber
    WHERE s.member_count IS DISTINCT FROM g.fact_count
)

SELECT 'fact_table_not_empty' AS check_name
     , CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
     , COUNT(*) AS observed
     , 'rows in analytics.member_term' AS detail
FROM analytics.member_term

UNION ALL
SELECT 'natural_key_is_unique'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , '(congress, bioguide_id) combinations appearing more than once'
FROM duplicate_natural_key

UNION ALL
SELECT 'no_future_start_dates'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'member terms starting after today'
FROM analytics.member_term
WHERE start_date > CURRENT_DATE

UNION ALL
SELECT 'age_years_within_range'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'rows with age_years outside [18, 105]'
FROM analytics.member_term
WHERE age_years < 18 OR age_years > 105

UNION ALL
SELECT 'birthday_precedes_start_date'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'rows where birthday is not before start_date'
FROM analytics.member_term
WHERE birthday >= start_date

UNION ALL
SELECT 'chamber_values_valid'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'rows whose chamber is neither House nor Senate'
FROM analytics.member_term
WHERE chamber NOT IN ('House', 'Senate')

UNION ALL
SELECT 'all_staged_rows_loaded'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'staged natural keys missing from the fact table'
FROM staged_not_loaded

UNION ALL
SELECT 'staged_values_match_fact'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'staged keys whose fact values differ from the staged values'
FROM staged_value_mismatch

UNION ALL
SELECT 'summary_reconciles_with_fact'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , '(congress, chamber) groups where the summary count does not match the fact table'
FROM summary_vs_fact

ORDER BY check_name;
