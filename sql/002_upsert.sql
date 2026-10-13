-- Idempotent upsert: staging -> analytics.member_term.
--
-- Three guarantees:
--   1. New member-terms are inserted.
--   2. Corrected member-terms (same natural key, any field changed) are
--      updated in place.
--   3. Unchanged rows are not written at all. The WHERE clause on DO UPDATE
--      filters them out, so `updated_at` stays stable across a no-op rerun.
--      "No duplicates" is the weak property; "nothing was touched" is the one
--      this statement provides.
--
-- DISTINCT ON collapses any same-key rows inside the batch before they reach
-- the target, highest source_row_number winning. Without it, a duplicated
-- input row would make ON CONFLICT fail with
-- "ON CONFLICT DO UPDATE command cannot affect row a second time".
--
-- The RETURNING clause uses `xmax = 0` to tell inserts from updates: on a
-- freshly inserted row the system column xmax is 0, while an updated row
-- carries the id of the updating transaction. That is how the run summary
-- reports inserted and updated separately from one statement.

WITH batch AS (
    SELECT DISTINCT ON (congress, bioguide_id)
          congress
        , bioguide_id
        , start_date
        , chamber
        , state_abbrev
        , party_code
        , party_label
        , bioname
        , birthday
        , cumulative_congresses
        , cumulative_chamber
        , age_days
        , age_years
        , generation
    FROM staging.member_term_stage
    ORDER BY congress, bioguide_id, source_row_number DESC
)
INSERT INTO analytics.member_term AS target (
      congress
    , bioguide_id
    , start_date
    , chamber
    , state_abbrev
    , party_code
    , party_label
    , bioname
    , birthday
    , cumulative_congresses
    , cumulative_chamber
    , age_days
    , age_years
    , generation
)
SELECT
      congress
    , bioguide_id
    , start_date
    , chamber
    , state_abbrev
    , party_code
    , party_label
    , bioname
    , birthday
    , cumulative_congresses
    , cumulative_chamber
    , age_days
    , age_years
    , generation
FROM batch
ON CONFLICT ON CONSTRAINT member_term_natural_key DO UPDATE SET
      start_date            = EXCLUDED.start_date
    , chamber               = EXCLUDED.chamber
    , state_abbrev          = EXCLUDED.state_abbrev
    , party_code            = EXCLUDED.party_code
    , party_label           = EXCLUDED.party_label
    , bioname               = EXCLUDED.bioname
    , birthday              = EXCLUDED.birthday
    , cumulative_congresses = EXCLUDED.cumulative_congresses
    , cumulative_chamber    = EXCLUDED.cumulative_chamber
    , age_days              = EXCLUDED.age_days
    , age_years             = EXCLUDED.age_years
    , generation            = EXCLUDED.generation
    , updated_at            = now()
WHERE (
      target.start_date
    , target.chamber
    , target.state_abbrev
    , target.party_code
    , target.party_label
    , target.bioname
    , target.birthday
    , target.cumulative_congresses
    , target.cumulative_chamber
    , target.age_days
    , target.age_years
    , target.generation
) IS DISTINCT FROM (
      EXCLUDED.start_date
    , EXCLUDED.chamber
    , EXCLUDED.state_abbrev
    , EXCLUDED.party_code
    , EXCLUDED.party_label
    , EXCLUDED.bioname
    , EXCLUDED.birthday
    , EXCLUDED.cumulative_congresses
    , EXCLUDED.cumulative_chamber
    , EXCLUDED.age_days
    , EXCLUDED.age_years
    , EXCLUDED.generation
)
RETURNING (xmax = 0) AS was_inserted;
