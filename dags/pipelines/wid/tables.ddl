/*
========================================================================================
SQL RESOURCES FOR WID (WORLD INEQUALITY DATABASE) DATA
========================================================================================
Description: This script creates database tables and other SQL resources for storing
             and analyzing income and wealth distribution data from WID.world.
             Tracks pre-tax national income shares and net personal wealth shares
             across percentile groups for all countries and years.

Tables:
  - wid.country: Country/entity dimension.
  - wid.variable: WID variable dimension (sixlet codes).
  - wid.percentile: Distribution percentile dimension.
  - wid.observation: Fact table with distribution values.
  - wid.data_quality: Per-country quality metadata.

Prerequisites:
  - The wid schema must already exist (created by schemas.ddl).

Connection:
  - psql -U postgres -d system2 -f pipelines/wid/tables.ddl
========================================================================================
*/


----------------------------------------------------------------------------------------

-- Country and entity dimension table sourced from WID.world.
CREATE TABLE IF NOT EXISTS wid.country (
    -- Identification.
    country_code TEXT PRIMARY KEY

    -- Attributes.
    , name TEXT

    -- Audit fields.
    , create_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
    , update_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for country table.
CREATE INDEX IF NOT EXISTS country_name_idx
ON wid.country (name);
CREATE INDEX IF NOT EXISTS country_create_ts_brin_idx
ON wid.country USING brin (create_ts);

-- Table comment.
COMMENT ON TABLE wid.country IS
'Country and entity dimension table sourced from WID.world. Includes '
'sovereign nations, sub-national regions, and aggregate groupings.';

-- Column comments.
COMMENT ON COLUMN wid.country.country_code IS
'WID entity code: two-letter ISO 3166-1 alpha-2 for countries, '
'hyphenated codes for sub-national regions (e.g. US-CA) and '
'aggregate groupings (e.g. OA-MER).';
COMMENT ON COLUMN wid.country.name IS
'Entity name in English, sourced from WID metadata API.';
COMMENT ON COLUMN wid.country.create_ts IS
'Timestamp when the record was created in the database.';
COMMENT ON COLUMN wid.country.update_ts IS
'Timestamp when the record was last updated in the database.';


----------------------------------------------------------------------------------------

-- WID variable dimension table defining the distribution metrics tracked.
CREATE TABLE IF NOT EXISTS wid.variable (
    -- Identification.
    variable_code TEXT PRIMARY KEY

    -- WID code components.
    , concept TEXT NOT NULL
    , series_type TEXT NOT NULL

    -- Descriptive metadata.
    , short_name TEXT NOT NULL
    , description TEXT
    , technical_description TEXT
    , unit TEXT NOT NULL
    , population_type TEXT
    , age_group TEXT

    -- Audit fields.
    , create_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
    , update_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table comment.
COMMENT ON TABLE wid.variable IS
'WID variable dimension table. Each row defines a distribution metric '
'tracked by this pipeline (e.g. pre-tax income share, wealth share). '
'Descriptions are sourced from the WID metadata API.';

-- Column comments.
COMMENT ON COLUMN wid.variable.variable_code IS
'WID sixlet code: concatenation of series_type + concept '
'(e.g. sptinc = s + ptinc, meaning share of pre-tax national '
'income). Primary key used across observation and data_quality.';
COMMENT ON COLUMN wid.variable.concept IS
'Five-letter WID concept identifying the economic quantity '
'(e.g. ptinc = pre-tax national income, hweal = net personal '
'wealth). Shared across series types: sptinc and aptinc both '
'have concept ptinc.';
COMMENT ON COLUMN wid.variable.series_type IS
'One-letter WID series type indicating the statistical measure '
'applied to the concept (s=share, a=average, t=threshold, '
'g=gini, m=total, w=ratio).';
COMMENT ON COLUMN wid.variable.short_name IS
'Human-readable short name for the variable.';
COMMENT ON COLUMN wid.variable.description IS
'Plain-English description of the variable from WID.';
COMMENT ON COLUMN wid.variable.technical_description IS
'Technical formula or decomposition from WID.';
COMMENT ON COLUMN wid.variable.unit IS
'Measurement unit (e.g. share, local currency).';
COMMENT ON COLUMN wid.variable.population_type IS
'WID pop code: statistical unit used for ranking and '
'attribution (j=equal-split adults, i=individuals, '
't=tax units, m=males, f=females, e=employed).';
COMMENT ON COLUMN wid.variable.age_group IS
'WID age code: 3-digit code defining the age demographic '
'included in the distribution (992=adults 20+, 999=all ages, '
'996=working-age 20-64).';
COMMENT ON COLUMN wid.variable.create_ts IS
'Timestamp when the record was created in the database.';
COMMENT ON COLUMN wid.variable.update_ts IS
'Timestamp when the record was last updated in the database.';


----------------------------------------------------------------------------------------

-- Distribution percentile dimension table.
CREATE TABLE IF NOT EXISTS wid.percentile (
    -- Identification.
    percentile_code TEXT PRIMARY KEY

    -- Range definition.
    , lower_bound NUMERIC(6, 2) NOT NULL
    , upper_bound NUMERIC(6, 2) NOT NULL
    , width NUMERIC(6, 2) NOT NULL

    -- Classification.
    , granularity TEXT NOT NULL

    -- Audit fields.
    , create_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
    , update_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()

    , CHECK (upper_bound > lower_bound)
    , CHECK (width > 0)
    , CHECK (granularity IN (
        'decile', 'percentile', 'mille', 'basis_point'
    ))
);

-- Indexes for percentile table.
CREATE INDEX IF NOT EXISTS percentile_granularity_idx
ON wid.percentile (granularity);
CREATE INDEX IF NOT EXISTS percentile_lower_bound_idx
ON wid.percentile (lower_bound);

-- Table comment.
COMMENT ON TABLE wid.percentile IS
'Distribution percentile dimension table. Defines the population '
'brackets used in WID distribution data with progressively finer '
'granularity toward the top of the distribution: deciles (10%), '
'percentiles (1%), milles (0.1%), and basis points (0.01%).';

-- Column comments.
COMMENT ON COLUMN wid.percentile.percentile_code IS
'WID percentile range code (e.g. p0p10 = bottom 10%, '
'p99.99p100 = top 0.01%).';
COMMENT ON COLUMN wid.percentile.lower_bound IS
'Lower bound of the percentile range (0-100 scale).';
COMMENT ON COLUMN wid.percentile.upper_bound IS
'Upper bound of the percentile range (0-100 scale).';
COMMENT ON COLUMN wid.percentile.width IS
'Width of the percentile range in percentage points.';
COMMENT ON COLUMN wid.percentile.granularity IS
'Granularity level: decile (10%), percentile (1%), '
'mille (0.1%), or basis_point (0.01%).';
COMMENT ON COLUMN wid.percentile.create_ts IS
'Timestamp when the record was created in the database.';
COMMENT ON COLUMN wid.percentile.update_ts IS
'Timestamp when the record was last updated in the database.';


----------------------------------------------------------------------------------------

-- Fact table storing distribution values per country, variable,
-- percentile, and year.
CREATE TABLE IF NOT EXISTS wid.observation (
    -- Composite key columns.
    country_code TEXT NOT NULL REFERENCES wid.country (country_code)
    , variable_code TEXT NOT NULL REFERENCES wid.variable (variable_code)
    , percentile_code TEXT NOT NULL
    REFERENCES wid.percentile (percentile_code)
    , year INTEGER NOT NULL

    -- Measurement.
    , value NUMERIC(10, 6)

    -- Audit fields.
    , create_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
    , update_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for observation table.
CREATE UNIQUE INDEX IF NOT EXISTS
observation_country_code_variable_code_percentile_code_year_idx
ON wid.observation (country_code, variable_code, percentile_code, year);
CREATE INDEX IF NOT EXISTS observation_year_idx
ON wid.observation (year);
CREATE INDEX IF NOT EXISTS observation_create_ts_brin_idx
ON wid.observation USING brin (create_ts);

-- Table comment.
COMMENT ON TABLE wid.observation IS
'Fact table storing income and wealth distribution values from '
'WID.world. Each row records the share of total income or wealth '
'held by a given percentile group in a given country and year.';

-- Column comments.
COMMENT ON COLUMN wid.observation.country_code IS
'References wid.country(country_code). Identifies the country '
'or entity this observation belongs to.';
COMMENT ON COLUMN wid.observation.variable_code IS
'References wid.variable(variable_code). Identifies the '
'distribution metric (e.g. sptinc, shweal).';
COMMENT ON COLUMN wid.observation.percentile_code IS
'References wid.percentile(percentile_code). Identifies the '
'population bracket (e.g. p99p100 = top 1%).';
COMMENT ON COLUMN wid.observation.year IS
'Calendar year of the observation.';
COMMENT ON COLUMN wid.observation.value IS
'Distribution value. For share variables, a fraction where '
'0.19 means 19% of total income or wealth.';
COMMENT ON COLUMN wid.observation.create_ts IS
'Timestamp when the record was created in the database.';
COMMENT ON COLUMN wid.observation.update_ts IS
'Timestamp when the record was last updated in the database.';


----------------------------------------------------------------------------------------

-- Data quality metadata per country and variable combination.
CREATE TABLE IF NOT EXISTS wid.data_quality (
    -- Composite key columns.
    country_code TEXT NOT NULL REFERENCES wid.country (country_code)
    , variable_code TEXT NOT NULL
    REFERENCES wid.variable (variable_code)

    -- Quality indicators.
    , quality_score INTEGER
    , imputation TEXT
    , extrapolation_ranges TEXT

    -- Audit fields.
    , create_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
    , update_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()

    -- Constraints.
    , CHECK (quality_score BETWEEN 1 AND 5)
);

-- Indexes for data_quality table.
CREATE UNIQUE INDEX IF NOT EXISTS
data_quality_country_code_variable_code_idx
ON wid.data_quality (country_code, variable_code);
CREATE INDEX IF NOT EXISTS data_quality_quality_score_idx
ON wid.data_quality (quality_score);

-- Table comment.
COMMENT ON TABLE wid.data_quality IS
'Data quality metadata from WID per country and variable. Describes '
'estimation methodology, imputation approach, and which year ranges '
'are extrapolated rather than directly observed.';

-- Column comments.
COMMENT ON COLUMN wid.data_quality.country_code IS
'References wid.country(country_code).';
COMMENT ON COLUMN wid.data_quality.variable_code IS
'References wid.variable(variable_code).';
COMMENT ON COLUMN wid.data_quality.quality_score IS
'WID data quality score from 1 (lowest) to 5 (highest). '
'Higher scores indicate more reliable underlying data sources.';
COMMENT ON COLUMN wid.data_quality.imputation IS
'Imputation method used by WID (e.g. full, tax and survey, '
'survey, tax).';
COMMENT ON COLUMN wid.data_quality.extrapolation_ranges IS
'JSON-encoded list of year ranges where values are extrapolated '
'rather than directly estimated (e.g. [[1820, 1910]]).';
COMMENT ON COLUMN wid.data_quality.create_ts IS
'Timestamp when the record was created in the database.';
COMMENT ON COLUMN wid.data_quality.update_ts IS
'Timestamp when the record was last updated in the database.';
