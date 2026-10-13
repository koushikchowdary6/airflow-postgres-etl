-- Example analytical queries. Not run by the pipeline.
--
-- Run them with:  python -m etl.cli examples
-- or paste into psql:  make psql

-- 1. The headline trend: median age of each chamber over time, with the
--    change against the previous Congress.
SELECT
      congress
    , chamber
    , member_count
    , median_age_years
    , median_change_vs_prev
    , pct_aged_65_plus
FROM analytics.congress_age_summary
ORDER BY congress DESC, chamber
LIMIT 20;

-- 2. The ten oldest Congresses by Senate median age.
SELECT congress, median_age_years, oldest_member
FROM analytics.congress_age_summary
WHERE chamber = 'Senate'
ORDER BY median_age_years DESC
LIMIT 10;

-- 3. Longest-serving members, by number of Congresses served.
--    Uses the bioguide_id index rather than scanning per member.
SELECT
      bioguide_id
    , MAX(bioname)       AS bioname
    , COUNT(*)           AS congresses_served
    , MIN(congress)      AS first_congress
    , MAX(congress)      AS last_congress
    , ROUND(MAX(age_years), 1) AS age_at_last_term
FROM analytics.member_term
GROUP BY bioguide_id
ORDER BY congresses_served DESC, bioguide_id
LIMIT 15;

-- 4. Party composition of the most recent Congress in the warehouse.
WITH latest AS (
    SELECT MAX(congress) AS congress FROM analytics.member_term
)
SELECT
      m.chamber
    , m.party_label
    , COUNT(*) AS members
    , ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY m.chamber), 1) AS pct_of_chamber
FROM analytics.member_term AS m
JOIN latest AS l ON l.congress = m.congress
GROUP BY m.chamber, m.party_label
ORDER BY m.chamber, members DESC;

-- 5. Generational turnover: share of each Congress by generation, House only.
SELECT
      congress
    , generation
    , COUNT(*) AS members
    , ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY congress), 1) AS pct_of_congress
FROM analytics.member_term
WHERE chamber = 'House' AND congress >= 110
GROUP BY congress, generation
ORDER BY congress DESC, members DESC
LIMIT 25;

-- 6. Operational: the last ten pipeline runs and what they did.
SELECT
      batch_id
    , status
    , rows_read
    , rows_staged
    , rows_rejected
    , rows_inserted
    , rows_updated
    , max_congress
    , started_at
    , finished_at
FROM etl.batch_run
ORDER BY started_at DESC
LIMIT 10;

-- 7. Operational: why rows were rejected in the most recent batch.
SELECT column_name, reason, COUNT(*) AS occurrences
FROM etl.rejected_row
WHERE batch_id = (SELECT batch_id FROM etl.batch_run ORDER BY started_at DESC LIMIT 1)
GROUP BY column_name, reason
ORDER BY occurrences DESC;
