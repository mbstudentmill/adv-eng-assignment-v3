-- Advanced Data Engineering Assignment - Task 2
-- BigQuery Data Warehouse DDL for IMDb Star Schema (Fixed Version)
-- This creates a professional-grade data warehouse with partitioning and clustering

-- Create the dataset (BigQuery uses CREATE SCHEMA for datasets)
-- Note: Dataset already created by connection test, proceeding with table creation

-- 1. DIMENSION TABLE: dim_title (Title Information)
CREATE OR REPLACE TABLE `ade-adveng-assign.imdb_warehouse.dim_title` (
  tconst STRING NOT NULL,
  title_type STRING,
  primary_title STRING,
  original_title STRING,
  is_adult BOOLEAN,
  start_year INT64,
  end_year INT64,
  runtime_minutes INT64,
  genres ARRAY<STRING>,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1888, 2030, 1))
CLUSTER BY title_type, genres
OPTIONS(
  description = 'Title dimension table with partitioning by start_year and clustering by title_type and genres',
  labels = [('table_type', 'dimension'), ('subject', 'title')]
);

-- 2. DIMENSION TABLE: dim_person (Person Information)
CREATE OR REPLACE TABLE `ade-adveng-assign.imdb_warehouse.dim_person` (
  nconst STRING NOT NULL,
  primary_name STRING,
  birth_year INT64,
  death_year INT64,
  primary_profession ARRAY<STRING>,
  known_for_titles ARRAY<STRING>,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY primary_profession
OPTIONS(
  description = 'Person dimension table with clustering by primary_profession',
  labels = [('table_type', 'dimension'), ('subject', 'person')]
);

-- 3. DIMENSION TABLE: dim_genre (Genre Information)
CREATE OR REPLACE TABLE `ade-adveng-assign.imdb_warehouse.dim_genre` (
  genre_id STRING NOT NULL,
  genre_name STRING NOT NULL,
  genre_description STRING,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description = 'Genre dimension table for normalized genre information',
  labels = [('table_type', 'dimension'), ('subject', 'genre')]
);

-- 4. DIMENSION TABLE: dim_region (Region Information)
CREATE OR REPLACE TABLE `ade-adveng-assign.imdb_warehouse.dim_region` (
  region_id STRING NOT NULL,
  region_name STRING NOT NULL,
  language STRING,
  country STRING,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description = 'Region dimension table for title availability by region',
  labels = [('table_type', 'dimension'), ('subject', 'region')]
);

-- 5. DIMENSION TABLE: dim_date (Date Information)
CREATE OR REPLACE TABLE `ade-adveng-assign.imdb_warehouse.dim_date` (
  date_id STRING NOT NULL,
  full_date DATE NOT NULL,
  year INT64 NOT NULL,
  month INT64 NOT NULL,
  day INT64 NOT NULL,
  quarter INT64 NOT NULL,
  day_of_week INT64 NOT NULL,
  day_name STRING NOT NULL,
  month_name STRING NOT NULL,
  is_weekend BOOLEAN NOT NULL,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(1888, 2030, 1))
OPTIONS(
  description = 'Date dimension table with partitioning by year',
  labels = [('table_type', 'dimension'), ('subject', 'date')]
);

-- 6. BRIDGE TABLE: bridge_title_genre (Many-to-Many Relationship)
CREATE OR REPLACE TABLE `ade-adveng-assign.imdb_warehouse.bridge_title_genre` (
  tconst STRING NOT NULL,
  genre_id STRING NOT NULL,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY tconst, genre_id
OPTIONS(
  description = 'Bridge table for many-to-many relationship between titles and genres',
  labels = [('table_type', 'bridge'), ('subject', 'title_genre')]
);

-- 7. FACT TABLE: fact_title_rating (Central Fact Table)
CREATE OR REPLACE TABLE `ade-adveng-assign.imdb_warehouse.fact_title_rating` (
  fact_id STRING NOT NULL,
  tconst STRING NOT NULL,
  nconst STRING,  -- For person ratings
  average_rating FLOAT64,
  num_votes INT64,
  start_year INT64,
  genre STRING,
  region STRING,
  rating_date DATE,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY RANGE_BUCKET(start_year, GENERATE_ARRAY(1888, 2030, 1))
CLUSTER BY tconst, genre, average_rating
OPTIONS(
  description = 'Fact table for title ratings with partitioning by start_year and clustering by tconst, genre, and average_rating',
  labels = [('table_type', 'fact'), ('subject', 'rating')]
);

-- Insert sample data into dimension tables
INSERT INTO `ade-adveng-assign.imdb_warehouse.dim_genre` (genre_id, genre_name, genre_description)
VALUES
  ('action', 'Action', 'Action films and television shows'),
  ('adventure', 'Adventure', 'Adventure and exploration content'),
  ('comedy', 'Comedy', 'Comedic and humorous content'),
  ('drama', 'Drama', 'Dramatic and serious content'),
  ('horror', 'Horror', 'Horror and frightening content'),
  ('romance', 'Romance', 'Romantic and love stories'),
  ('sci_fi', 'Science Fiction', 'Science fiction and futuristic content'),
  ('thriller', 'Thriller', 'Suspenseful and thrilling content');

-- Create views for common query patterns
CREATE OR REPLACE VIEW `ade-adveng-assign.imdb_warehouse.v_top_rated_movies` AS
SELECT 
  t.primary_title,
  t.start_year,
  t.genres,
  f.average_rating,
  f.num_votes
FROM `ade-adveng-assign.imdb_warehouse.fact_title_rating` f
JOIN `ade-adveng-assign.imdb_warehouse.dim_title` t ON f.tconst = t.tconst
WHERE f.num_votes >= 1000
  AND f.average_rating >= 8.0
  AND t.title_type = 'movie'
ORDER BY f.average_rating DESC, f.num_votes DESC;

-- Create view for genre analysis
CREATE OR REPLACE VIEW `ade-adveng-assign.imdb_warehouse.v_genre_performance` AS
SELECT 
  g.genre_name,
  COUNT(DISTINCT f.tconst) as title_count,
  AVG(f.average_rating) as avg_rating,
  SUM(f.num_votes) as total_votes
FROM `ade-adveng-assign.imdb_warehouse.fact_title_rating` f
JOIN `ade-adveng-assign.imdb_warehouse.dim_genre` g ON f.genre = g.genre_id
WHERE f.num_votes >= 100
GROUP BY g.genre_name
ORDER BY avg_rating DESC;

-- Create table for monitoring and audit
CREATE OR REPLACE TABLE `ade-adveng-assign.imdb_warehouse.etl_audit_log` (
  log_id STRING NOT NULL,
  table_name STRING NOT NULL,
  operation STRING NOT NULL,
  records_processed INT64,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status STRING,
  error_message STRING,
  created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
  description = 'Audit log for ETL operations',
  labels = [('table_type', 'audit'), ('subject', 'etl_log')]
);

-- Final status message
SELECT 'IMDb Data Warehouse created successfully!' as status;
