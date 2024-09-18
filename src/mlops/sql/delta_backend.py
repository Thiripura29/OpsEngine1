from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


class DeltaBackend:
    def __init__(self, spark, delta_config):
        self.spark = spark
        self.audit_table_path = delta_config["audit_table_path"]
        self.control_table_path = delta_config["control_table_path"]
        self.audit_table_name = delta_config["audit_table_name"]
        self.control_table_name = delta_config["control_table_name"]

    def create_audit_table_if_not_exists(self):
        """Create audit table in Delta Lake if it does not exist."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.audit_table_name} (
                audit_id STRING,
                job_name STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status STRING,
                record_count INT,
                error_message STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                additional_meta_data MAP<STRING,STRING>
            ) USING delta
            LOCATION {self.audit_table_path};
        """)
        print("Audit table created or verified.")

    def create_control_table_if_not_exists(self):
        """Create control table in Delta Lake if it does not exist."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.control_table_name} (
                control_id STRING,
                last_processed_datetime TIMESTAMP,
                next_scheduled_run TIMESTAMP,
                next_processed_id INT,
                frequency STRING,
                is_active BOOLEAN,
                partitions STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
            ) USING delta
            LOCATION {self.control_table_path};
        """)
        print("Control table created or verified.")

    def insert_audit_record(self, data):
        """Insert a record into the Delta Lake audit table using SQL."""
        insert_sql = f"""
            INSERT INTO delta.`{self.audit_table_path}`
            VALUES ('{data["job_name"]}', '{data["start_time"]}', '{data["end_time"]}',
                    '{data["status"]}',
                    {data["record_count"]}, {data["error_count"]},
                    '{data["error_message"]}', current_timestamp(), current_timestamp(),{data["additional_meta_data"]});
        """
        self.spark.sql(insert_sql)
        print("Inserted audit record.")

    def insert_control_record(self, data):
        """Insert a record into the Delta Lake control table using SQL."""
        insert_sql = f"""
            INSERT INTO delta.`{self.control_table_path}`
            VALUES ('{data["control_id"]}', '{data["last_processed_datetime"]}', '{data["next_scheduled_run"]}',
                    '{data["frequency"]}', {data["is_active"]}, current_timestamp(), current_timestamp());
        """
        self.spark.sql(insert_sql)
        print("Inserted control record.")

    def update_control_record(self, control_id, data):
        """Update a record in the Delta Lake control table using MERGE."""

        merge_sql = f"""
            MERGE INTO delta.`{self.control_table_path}` AS target
            USING (SELECT '{control_id}' AS control_id) AS source
            ON target.control_id = source.control_id
            WHEN MATCHED THEN UPDATE SET
                last_processed_datetime = '{data["last_processed_datetime"]}',
                next_scheduled_run = '{data["next_scheduled_run"]}',
                updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (control_id, last_processed_datetime, next_scheduled_run, 
                frequency, is_active, created_at, updated_at)
            VALUES ('{data["control_id"]}', '{data["last_processed_datetime"]}', '{data["next_scheduled_run"]}', 
                '{data["frequency"]}', {data["is_active"]}, current_timestamp(), current_timestamp());
        """
        self.spark.sql(merge_sql)
        print("Control record updated or inserted.")

    # def delete_audit_record(self, job_name):
    #     """Delete a record from the Delta Lake audit table using SQL-driven approach."""
    #     delete_sql = f"""
    #         MERGE INTO delta.`{self.audit_table_path}` AS target
    #         USING (SELECT '{job_name}' AS job_name) AS source
    #         ON target.job_name = source.job_name
    #         WHEN MATCHED THEN DELETE;
    #     """
    #     self.spark.sql(delete_sql)
    #     print("Audit record deleted.")
    #
    # def delete_control_record(self, control_id):
    #     """Delete a record from the Delta Lake control table using SQL-driven approach."""
    #     delete_sql = f"""
    #         MERGE INTO delta.`{self.control_table_path}` AS target
    #         USING (SELECT '{control_id}' AS control_id) AS source
    #         ON target.control_id = source.control_id
    #         WHEN MATCHED THEN DELETE;
    #     """
    #     self.spark.sql(delete_sql)
    #     print("Control record deleted.")

    def optimize_table(self, table_path):
        """Optimize the Delta table to compact small files and improve performance."""
        self.spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY (control_id);")
        print(f"Optimized table at {table_path}.")
