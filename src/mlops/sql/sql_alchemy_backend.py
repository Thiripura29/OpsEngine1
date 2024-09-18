from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class SQLAlchemyBackend:
    def __init__(self, connection_string, db_type):
        self.engine = create_engine(connection_string)
        self.db_type = db_type
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def create_audit_table_if_not_exists(self):
        """Create audit table in SQL databases."""
        ddl = {
            "mysql": """
                CREATE TABLE IF NOT EXISTS audit_table (
                    job_name VARCHAR(255),
                    start_time DATETIME,
                    end_time DATETIME,
                    status VARCHAR(50),
                    last_processed_datetime DATETIME,
                    record_count INT,
                    error_count INT,
                    error_message VARCHAR(255),
                    created_at DATETIME,
                    updated_at DATETIME
                );
            """,
            "postgresql": """
                CREATE TABLE IF NOT EXISTS audit_table (
                    job_name VARCHAR(255),
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status VARCHAR(50),
                    last_processed_datetime TIMESTAMP,
                    record_count INT,
                    error_count INT,
                    error_message VARCHAR(255),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                );
            """
        }
        with self.engine.connect() as connection:
            connection.execute(text(ddl[self.db_type]))

    def create_control_table_if_not_exists(self):
        """Create control table in SQL databases."""
        ddl = {
            "mysql": """
                CREATE TABLE IF NOT EXISTS control_table (
                    control_id VARCHAR(255) PRIMARY KEY,
                    last_processed_datetime DATETIME,
                    next_scheduled_run DATETIME,
                    frequency VARCHAR(50),
                    is_active BOOLEAN,
                    created_at DATETIME,
                    updated_at DATETIME
                );
            """,
            "postgresql": """
                CREATE TABLE IF NOT EXISTS control_table (
                    control_id VARCHAR(255) PRIMARY KEY,
                    last_processed_datetime TIMESTAMP,
                    next_scheduled_run TIMESTAMP,
                    frequency VARCHAR(50),
                    is_active BOOLEAN,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                );
            """
        }
        with self.engine.connect() as connection:
            connection.execute(text(ddl[self.db_type]))

    def insert_audit_record(self, values):
        """Insert a record into the audit table."""
        insert_sql = """
            INSERT INTO audit_table (job_name, start_time, end_time, status, last_processed_datetime, 
            record_count, error_count, error_message, created_at, updated_at) 
            VALUES (:job_name, :start_time, :end_time, :status, :last_processed_datetime, 
            :record_count, :error_count, :error_message, :created_at, :updated_at);
        """
        with self.engine.connect() as connection:
            connection.execute(text(insert_sql), **{
                'job_name': values[0],
                'start_time': values[1],
                'end_time': values[2],
                'status': values[3],
                'last_processed_datetime': values[4],
                'record_count': values[5],
                'error_count': values[6],
                'error_message': values[7],
                'created_at': values[8],
                'updated_at': values[9]
            })

    def insert_control_record(self, values):
        """Insert a record into the control table."""
        insert_sql = """
            INSERT INTO control_table (control_id, last_processed_datetime, next_scheduled_run, 
            frequency, is_active, created_at, updated_at)
            VALUES (:control_id, :last_processed_datetime, :next_scheduled_run, 
            :frequency, :is_active, :created_at, :updated_at);
        """
        with self.engine.connect() as connection:
            connection.execute(text(insert_sql), **{
                'control_id': values[0],
                'last_processed_datetime': values[1],
                'next_scheduled_run': values[2],
                'frequency': values[3],
                'is_active': values[4],
                'created_at': values[5],
                'updated_at': values[6]
            })

    def update_control_record(self, control_id, values):
        """Update a record in the control table."""
        update_sql = """
            UPDATE control_table
            SET last_processed_datetime = :last_processed_datetime, next_scheduled_run = :next_scheduled_run, frequency = :frequency, 
            is_active = :is_active, updated_at = :updated_at
            WHERE control_id = :control_id;
        """
        with self.engine.connect() as connection:
            connection.execute(text(update_sql), **{
                'last_processed_datetime': values[0],
                'next_scheduled_run': values[1],
                'frequency': values[2],
                'is_active': values[3],
                'updated_at': values[4],
                'control_id': control_id
            })

    def delete_audit_record(self, job_name):
        """Delete a record from the audit table."""
        delete_sql = "DELETE FROM audit_table WHERE job_name = :job_name;"
        with self.engine.connect() as connection:
            connection.execute(text(delete_sql), {'job_name': job_name})

    def delete_control_record(self, control_id):
        """Delete a record from the control table."""
        delete_sql = "DELETE FROM control_table WHERE control_id = :control_id;"
        with self.engine.connect() as connection:
            connection.execute(text(delete_sql), {'control_id': control_id})
