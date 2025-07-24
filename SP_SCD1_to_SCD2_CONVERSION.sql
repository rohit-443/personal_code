CREATE OR REPLACE PROCEDURE SP_SCD2_DYNAMIC_LOAD(
    p_raw_table_name STRING,
    p_silver_table_name STRING,
    p_business_keys STRING  -- e.g. 'customer_id,region_id'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_keys_partition_clause STRING;
    v_keys_join_clause STRING;
    v_step1_sql STRING;
    v_step2_sql STRING;
    v_step3_sql STRING;
    v_step4_sql STRING;
BEGIN

  -- Build dynamic clauses
  LET v_keys_partition_clause = p_business_keys;
  LET v_keys_join_clause = ARRAY_TO_STRING(
    ARRAY_AGG(
      's.' || value || ' = sl.' || value
    ) OVER (),
    ' AND '
  )
  FROM TABLE(SPLIT_TO_TABLE(p_business_keys, ','));

  -- STEP 1: Create temp table with all raw except latest
  LET v_step1_sql = '
    CREATE OR REPLACE TEMP TABLE raw_except_latest AS
    SELECT *
    FROM ' || p_raw_table_name || '
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ' || v_keys_partition_clause || ' ORDER BY load_timestamp DESC) > 1;
  ';
  EXECUTE IMMEDIATE v_step1_sql;

  -- STEP 2: Update silver records with 2nd latest load_timestamp from raw
  LET v_step2_sql = '
    WITH second_latest_raw AS (
      SELECT ' || p_business_keys || ', load_timestamp AS second_latest_load_ts
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ' || v_keys_partition_clause || ' ORDER BY load_timestamp DESC) AS rn
        FROM ' || p_raw_table_name || '
      )
      WHERE rn = 2
    )
    UPDATE ' || p_silver_table_name || ' AS s
    SET 
      start_date = sl.second_latest_load_ts::DATE,
      end_date = NULL,
      is_current = TRUE
    FROM second_latest_raw AS sl
    WHERE ' || v_keys_join_clause || ';
  ';
  EXECUTE IMMEDIATE v_step2_sql;

  -- STEP 3: Insert historical versions
  LET v_step3_sql = '
    INSERT INTO ' || p_silver_table_name || ' (
      customer_id, name, address, phone,
      load_timestamp, start_date, end_date, is_current
    )
    SELECT 
      customer_id, name, address, phone,
      load_timestamp,
      load_timestamp::DATE AS start_date,
      DATEADD(DAY, -1, LEAD(load_timestamp) OVER (PARTITION BY ' || v_keys_partition_clause || ' ORDER BY load_timestamp))::DATE AS end_date,
      FALSE AS is_current
    FROM raw_except_latest;
  ';
  EXECUTE IMMEDIATE v_step3_sql;

  -- STEP 4: Handle single-record customers
  LET v_step4_sql = '
    WITH single_version_customers AS (
      SELECT *
      FROM (
        SELECT *,
               COUNT(*) OVER (PARTITION BY ' || v_keys_partition_clause || ') AS version_count,
               ROW_NUMBER() OVER (PARTITION BY ' || v_keys_partition_clause || ' ORDER BY load_timestamp DESC) AS rn
        FROM ' || p_raw_table_name || '
      )
      WHERE version_count = 1 AND rn = 1
    )
    INSERT INTO ' || p_silver_table_name || ' (
      customer_id, name, address, phone,
      load_timestamp, start_date, end_date, is_current
    )
    SELECT 
      customer_id, name, address, phone,
      load_timestamp,
      load_timestamp::DATE AS start_date,
      NULL AS end_date,
      TRUE AS is_current
    FROM single_version_customers;
  ';
  EXECUTE IMMEDIATE v_step4_sql;

  RETURN 'Dynamic SCD Type 2 Load Completed';

END;
$$;
