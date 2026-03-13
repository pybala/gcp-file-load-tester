"""
bigquery_client.py
------------------
Wrapper around google-cloud-bigquery providing all query helpers needed
by the validation framework.  All heavy computation is pushed into SQL
to avoid pulling large result sets to the client.
"""

import logging
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

logger = logging.getLogger(__name__)


class BigQueryClient:
    """
    Thin, reusable wrapper around the google-cloud-bigquery client.

    All methods accept generic parameters (table ref, column names, etc.)
    so no table-specific logic lives here.
    """

    def __init__(self, project: Optional[str] = None):
        """
        Initialise the BigQuery client.

        Args:
            project: GCP project ID.  If None the library uses the
                     environment's application-default credentials project.
        """
        self.client = bigquery.Client(project=project)
        logger.info("BigQuery client initialised (project=%s)", project or "default")

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def get_table_schema(self, dataset: str, table: str) -> List[SchemaField]:
        """
        Retrieve the schema (list of SchemaField) for a BigQuery table.

        Args:
            dataset: Dataset name.
            table:   Table name.

        Returns:
            List of google.cloud.bigquery.SchemaField objects.
        """
        table_ref = self.client.dataset(dataset).table(table)
        bq_table = self.client.get_table(table_ref)
        logger.debug("Fetched schema for %s.%s (%d fields)", dataset, table, len(bq_table.schema))
        return bq_table.schema

    # ------------------------------------------------------------------
    # Generic query execution
    # ------------------------------------------------------------------

    def run_query(self, sql: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> bigquery.table.RowIterator:
        """
        Execute a SQL query and return the RowIterator result.

        Args:
            sql:        SQL string.
            job_config: Optional QueryJobConfig (e.g. parameterised queries).

        Returns:
            BigQuery RowIterator.
        """
        logger.debug("Running query:\n%s", sql)
        job = self.client.query(sql, job_config=job_config)
        return job.result()

    def run_query_to_dict_list(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of plain dicts.

        Args:
            sql: SQL string.

        Returns:
            List of row dicts.
        """
        rows = self.run_query(sql)
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Row count
    # ------------------------------------------------------------------

    def get_row_count(self, dataset: str, table: str, where: str = "") -> int:
        """
        Return the number of rows in a BigQuery table.

        Args:
            dataset: Dataset name.
            table:   Table name.
            where:   Optional WHERE clause (without the WHERE keyword).

        Returns:
            Integer row count.
        """
        where_clause = f"WHERE {where}" if where else ""
        sql = f"SELECT COUNT(*) AS cnt FROM `{dataset}.{table}` {where_clause}"
        rows = self.run_query_to_dict_list(sql)
        return int(rows[0]["cnt"])

    # ------------------------------------------------------------------
    # Aggregate helpers
    # ------------------------------------------------------------------

    def get_aggregate_stats(
        self,
        dataset: str,
        table: str,
        column: str,
        functions: List[str],
    ) -> Dict[str, Any]:
        """
        Compute one or more aggregate functions on a single column.

        Supported functions: sum, min, max, avg, distinct_count.

        Args:
            dataset:   Dataset name.
            table:     Table name.
            column:    Column to aggregate.
            functions: List of function names.

        Returns:
            Dict mapping function_name → computed value.
        """
        func_map = {
            "sum":            f"SUM(CAST(`{column}` AS FLOAT64))",
            "min":            f"MIN(`{column}`)",
            "max":            f"MAX(`{column}`)",
            "avg":            f"AVG(CAST(`{column}` AS FLOAT64))",
            "distinct_count": f"COUNT(DISTINCT `{column}`)",
        }
        selects = []
        for fn in functions:
            if fn not in func_map:
                raise ValueError(f"Unsupported aggregate function: {fn}")
            selects.append(f"{func_map[fn]} AS {fn}")

        sql = f"SELECT {', '.join(selects)} FROM `{dataset}.{table}`"
        rows = self.run_query_to_dict_list(sql)
        return rows[0] if rows else {}

    # ------------------------------------------------------------------
    # Partition helpers
    # ------------------------------------------------------------------

    def get_partition_aggregates(
        self,
        dataset: str,
        table: str,
        partition_column: str,
        agg_column: str,
        agg_function: str = "COUNT(*)",
    ) -> List[Dict[str, Any]]:
        """
        Aggregate data grouped by partition column.

        Args:
            dataset:          Dataset name.
            table:            Table name.
            partition_column: Column used for grouping (partition key).
            agg_column:       Alias for the aggregated result column.
            agg_function:     SQL aggregate expression (default COUNT(*)).

        Returns:
            List of dicts with keys: partition_column value, agg_column value.
        """
        sql = (
            f"SELECT `{partition_column}`, {agg_function} AS {agg_column} "
            f"FROM `{dataset}.{table}` "
            f"GROUP BY `{partition_column}` "
            f"ORDER BY `{partition_column}`"
        )
        return self.run_query_to_dict_list(sql)

    # ------------------------------------------------------------------
    # Hash validation
    # ------------------------------------------------------------------

    def get_hash_aggregate(self, dataset: str, table: str) -> int:
        """
        Compute a deterministic hash aggregate over the entire table using
        FARM_FINGERPRINT applied to the JSON representation of each row.

        BigQuery's FARM_FINGERPRINT is deterministic for identical inputs,
        making it suitable for whole-table checksums.

        Args:
            dataset: Dataset name.
            table:   Table name.

        Returns:
            XOR-aggregate of all per-row FARM_FINGERPRINT values (INT64).
        """
        sql = (
            f"SELECT BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS hash_agg "
            f"FROM `{dataset}.{table}` AS t"
        )
        rows = self.run_query_to_dict_list(sql)
        return int(rows[0]["hash_agg"]) if rows and rows[0]["hash_agg"] is not None else 0

    # ------------------------------------------------------------------
    # Random sampling helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_pk_value(value: Any) -> str:
        """
        Format a primary key value for an inline SQL literal.

        Numeric values (int/float) are emitted without quotes so they are
        compatible with INT64 / FLOAT64 / NUMERIC BigQuery columns.
        String values are single-quoted with internal single-quotes escaped.
        None is emitted as NULL.

        Args:
            value: The raw value read from the CSV (may be str, int, or float).

        Returns:
            A SQL literal string (e.g. "42", "3.14", "'hello'", "NULL").
        """
        if value is None:
            return "NULL"
        # Native Python numeric types (not bool, which is a subclass of int)
        if isinstance(value, bool):
            return f"'{value}'"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            return str(value)
        # String value from CSV — attempt numeric coercion before quoting
        str_val = str(value).strip()
        # Integer check: only if the parsed int round-trips exactly
        try:
            int_parsed = int(str_val)
            if str(int_parsed) == str_val:
                return str(int_parsed)
        except (ValueError, TypeError):
            pass
        # Float check
        try:
            float(str_val)
            return str_val
        except (ValueError, TypeError):
            pass
        # Plain string — escape embedded single quotes
        return "'" + str_val.replace("'", "''") + "'"

    def get_rows_by_primary_keys(
        self,
        dataset: str,
        table: str,
        primary_keys: List[str],
        key_values: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Fetch specific rows from BigQuery by their primary key values.

        PK values are formatted as typed SQL literals (numeric columns are not
        quoted) so the query is compatible with INT64, FLOAT64, NUMERIC, and
        STRING BigQuery column types.

        Args:
            dataset:      Dataset name.
            table:        Table name.
            primary_keys: List of PK column names.
            key_values:   List of dicts, each mapping PK column → value.

        Returns:
            List of row dicts from BigQuery.
        """
        if not key_values:
            return []

        fmt = self._format_pk_value

        if len(primary_keys) == 1:
            pk = primary_keys[0]
            values_sql = ", ".join([fmt(row[pk]) for row in key_values])
            where = f"`{pk}` IN ({values_sql})"
        else:
            # Composite key: STRUCT comparison
            struct_cols = ", ".join([f"`{k}`" for k in primary_keys])
            struct_literals = []
            for row in key_values:
                vals = ", ".join([fmt(row[k]) for k in primary_keys])
                struct_literals.append(f"({vals})")
            where = f"({struct_cols}) IN ({', '.join(struct_literals)})"

        sql = f"SELECT * FROM `{dataset}.{table}` WHERE {where}"
        return self.run_query_to_dict_list(sql)

    # ------------------------------------------------------------------
    # Column distribution
    # ------------------------------------------------------------------

    def get_column_distribution(
        self,
        dataset: str,
        table: str,
        column: str,
    ) -> Dict[str, Any]:
        """
        Compute distribution statistics for a single column.

        Statistics: min, max, avg, stddev, null_count, total_count.

        Args:
            dataset: Dataset name.
            table:   Table name.
            column:  Column name.

        Returns:
            Dict with keys: min, max, avg, stddev, null_count, total_count.
        """
        sql = (
            f"SELECT "
            f"  MIN(CAST(`{column}` AS FLOAT64))    AS min_val, "
            f"  MAX(CAST(`{column}` AS FLOAT64))    AS max_val, "
            f"  AVG(CAST(`{column}` AS FLOAT64))    AS avg_val, "
            f"  STDDEV(CAST(`{column}` AS FLOAT64)) AS stddev_val, "
            f"  COUNTIF(`{column}` IS NULL)         AS null_count, "
            f"  COUNT(*)                             AS total_count "
            f"FROM `{dataset}.{table}`"
        )
        rows = self.run_query_to_dict_list(sql)
        return rows[0] if rows else {}

    # ------------------------------------------------------------------
    # Primary key helpers
    # ------------------------------------------------------------------

    def get_duplicate_pk_count(
        self,
        dataset: str,
        table: str,
        primary_keys: List[str],
    ) -> int:
        """
        Count rows in BigQuery where the primary key combination is not unique.

        Args:
            dataset:      Dataset name.
            table:        Table name.
            primary_keys: List of PK column names.

        Returns:
            Number of duplicate PK occurrences (0 means fully unique).
        """
        pk_cols = ", ".join([f"`{k}`" for k in primary_keys])
        sql = (
            f"SELECT COUNT(*) AS dup_count FROM ("
            f"  SELECT {pk_cols}, COUNT(*) AS cnt "
            f"  FROM `{dataset}.{table}` "
            f"  GROUP BY {pk_cols} "
            f"  HAVING cnt > 1"
            f")"
        )
        rows = self.run_query_to_dict_list(sql)
        return int(rows[0]["dup_count"]) if rows else 0

    # ------------------------------------------------------------------
    # Null validation helpers
    # ------------------------------------------------------------------

    def get_column_null_counts(
        self,
        dataset: str,
        table: str,
        columns: List[str],
    ) -> Dict[str, int]:
        """
        Count NULL values per column in a BigQuery table.

        Uses a single query with one ``COUNTIF(col IS NULL)`` expression per
        column, so the table is scanned only once regardless of column count.

        Works for all column types including STRUCT (RECORD) and ARRAY
        (REPEATED) — BigQuery's ``IS NULL`` test applies to the top-level
        field in all cases.

        Args:
            dataset: Dataset name.
            table:   Table name.
            columns: Column names to check.

        Returns:
            Dict mapping column_name → null count (0 if no NULLs).
        """
        if not columns:
            return {}
        selects = [
            f"COUNTIF(`{col}` IS NULL) AS null_cnt_{i}"
            for i, col in enumerate(columns)
        ]
        sql = f"SELECT {', '.join(selects)} FROM `{dataset}.{table}`"
        rows = self.run_query_to_dict_list(sql)
        if not rows:
            return {col: 0 for col in columns}
        return {
            col: int(rows[0].get(f"null_cnt_{i}", 0) or 0)
            for i, col in enumerate(columns)
        }

    # ------------------------------------------------------------------
    # Column-level checksum helpers
    # ------------------------------------------------------------------

    def get_column_checksums(
        self,
        dataset: str,
        table: str,
        columns: List[str],
    ) -> Dict[str, int]:
        """
        Compute a deterministic per-column hash aggregate using
        ``BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(column)))``.

        A single query is issued for all columns so the table is scanned
        only once.  Returns 0 for any column that is entirely NULL (because
        ``FARM_FINGERPRINT(NULL) = NULL`` is excluded from BIT_XOR).

        Works for all column types including STRUCT and ARRAY.

        Args:
            dataset: Dataset name.
            table:   Table name.
            columns: Column names to hash.

        Returns:
            Dict mapping column_name → BIT_XOR hash (int64, 0 if all-null).
        """
        if not columns:
            return {}
        selects = [
            f"BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(`{col}`))) AS chk_{i}"
            for i, col in enumerate(columns)
        ]
        sql = f"SELECT {', '.join(selects)} FROM `{dataset}.{table}`"
        rows = self.run_query_to_dict_list(sql)
        if not rows:
            return {col: 0 for col in columns}
        return {
            col: int(rows[0].get(f"chk_{i}", 0) or 0)
            for i, col in enumerate(columns)
        }

