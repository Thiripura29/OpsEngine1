from dataclasses import dataclass
from enum import Enum
from typing import Optional, List

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType


class WriteType(Enum):
    """Types of write operations."""

    OVERWRITE = "overwrite"
    COMPLETE = "complete"
    APPEND = "append"
    UPDATE = "update"
    MERGE = "merge"
    ERROR_IF_EXISTS = "error"
    IGNORE_IF_EXISTS = "ignore"


class SQLDefinitions(Enum):
    """SQL definitions statements."""

    compute_table_stats = "ANALYZE TABLE {} COMPUTE STATISTICS"
    drop_table_stmt = "DROP TABLE IF EXISTS"
    drop_view_stmt = "DROP VIEW IF EXISTS"
    truncate_stmt = "TRUNCATE TABLE"
    describe_stmt = "DESCRIBE TABLE"
    optimize_stmt = "OPTIMIZE"
    show_tbl_props_stmt = "SHOW TBLPROPERTIES"
    delete_where_stmt = "DELETE FROM {} WHERE {}"


class DQType(Enum):
    """Available data quality tasks."""

    VALIDATOR = "validator"


class OutputFormat(Enum):
    """Formats of algorithm output."""

    JDBC = "jdbc"
    AVRO = "avro"
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    DELTAFILES = "delta"
    KAFKA = "kafka"
    CONSOLE = "console"
    NOOP = "noop"
    DATAFRAME = "dataframe"
    FILE = "file"  # Internal use only
    TABLE = "table"  # Internal use only


@dataclass
class MergeOptions(object):
    """Options for a merge operation.

    - merge_predicate: predicate to apply to the merge operation so that we can
        check if a new record corresponds to a record already included in the
        historical data.
    - insert_only: indicates if the merge should only insert data (e.g., deduplicate
        scenarios).
    - delete_predicate: predicate to apply to the delete operation.
    - update_predicate: predicate to apply to the update operation.
    - insert_predicate: predicate to apply to the insert operation.
    - update_column_set: rules to apply to the update operation which allows to
        set the value for each column to be updated.
        (e.g. {"data": "new.data", "count": "current.count + 1"} )
    - insert_column_set: rules to apply to the insert operation which allows to
        set the value for each column to be inserted.
        (e.g. {"date": "updates.date", "count": "1"} )
    """

    merge_predicate: str
    insert_only: bool = False
    delete_predicate: Optional[str] = None
    update_predicate: Optional[str] = None
    insert_predicate: Optional[str] = None
    update_column_set: Optional[dict] = None
    insert_column_set: Optional[dict] = None


@dataclass
class DQFunctionSpec(object):
    """Defines a data quality function specification.

    - function - name of the data quality function (expectation) to execute.
    It follows the great_expectations api https://greatexpectations.io/expectations/.
    - args - args of the function (expectation). Follow the same api as above.
    """

    function: str
    args: Optional[dict] = None


class DQDefaults(Enum):
    """Defaults used on the data quality process."""

    FILE_SYSTEM_STORE = "file_system"
    FILE_SYSTEM_S3_STORE = "s3"
    DQ_BATCH_IDENTIFIERS = ["spec_id", "input_id", "timestamp"]
    DATASOURCE_CLASS_NAME = "Datasource"
    DATASOURCE_EXECUTION_ENGINE = "SparkDFExecutionEngine"
    DATA_CONNECTORS_CLASS_NAME = "RuntimeDataConnector"
    DATA_CONNECTORS_MODULE_NAME = "great_expectations.datasource.data_connector"
    DATA_CHECKPOINTS_CLASS_NAME = "SimpleCheckpoint"
    DATA_CHECKPOINTS_CONFIG_VERSION = 1.0
    STORE_BACKEND = "s3"
    EXPECTATIONS_STORE_PREFIX = "dq/expectations/"
    VALIDATIONS_STORE_PREFIX = "dq/validations/"
    DATA_DOCS_PREFIX = "dq/data_docs/site/"
    CHECKPOINT_STORE_PREFIX = "dq/checkpoints/"
    VALIDATION_COLUMN_IDENTIFIER = "validationresultidentifier"
    CUSTOM_EXPECTATION_LIST = [
        "expect_column_values_to_be_date_not_older_than",
        "expect_column_pair_a_to_be_smaller_or_equal_than_b",
        "expect_multicolumn_column_a_must_equal_b_or_c",
        "expect_queried_column_agg_value_to_be",
    ]
    DQ_VALIDATIONS_SCHEMA = StructType(
        [
            StructField(
                "dq_validations",
                StructType(
                    [
                        StructField("run_name", StringType()),
                        StructField("run_success", BooleanType()),
                        StructField("raised_exceptions", BooleanType()),
                        StructField("run_row_success", BooleanType()),
                        StructField(
                            "dq_failure_details",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("expectation_type", StringType()),
                                        StructField("kwargs", StringType()),
                                    ]
                                ),
                            ),
                        ),
                    ]
                ),
            )
        ]
    )


@dataclass
class DQSpec(object):
    """Data quality overall specification.

    - spec_id - id of the specification.
    - input_id - id of the input specification.
    - dq_type - type of DQ process to execute (e.g. validator).
    - dq_functions - list of function specifications to execute.
    - unexpected_rows_pk - the list of columns composing the primary key of the
        source data to identify the rows failing the DQ validations. Note: only one
        of tbl_to_derive_pk or unexpected_rows_pk arguments need to be provided. It
        is mandatory to provide one of these arguments when using tag_source_data
        as True. When tag_source_data is False, this is not mandatory, but still
        recommended.
    - tbl_to_derive_pk - db.table to automatically derive the unexpected_rows_pk from.
        Note: only one of tbl_to_derive_pk or unexpected_rows_pk arguments need to
        be provided. It is mandatory to provide one of these arguments when using
        tag_source_data as True. hen tag_source_data is False, this is not
        mandatory, but still recommended.
    - gx_result_format - great expectations result format. Default: "COMPLETE".
    - tag_source_data - when set to true, this will ensure that the DQ process ends by
        tagging the source data with an additional column with information about the
        DQ results. This column makes it possible to identify if the DQ run was
        succeeded in general and, if not, it unlocks the insights to know what
        specific rows have made the DQ validations fail and why. Default: False.
        Note: it only works if result_sink_explode is True, gx_result_format is
        COMPLETE, fail_on_error is False (which is done automatically when
        you specify tag_source_data as True) and tbl_to_derive_pk or
        unexpected_rows_pk is configured.
    - store_backend - which store_backend to use (e.g. s3 or file_system).
    - local_fs_root_dir - path of the root directory. Note: only applicable for
        store_backend file_system.
    - data_docs_local_fs - the path for data docs only for store_backend
        file_system.
    - bucket - the bucket name to consider for the store_backend (store DQ artefacts).
        Note: only applicable for store_backend s3.
    - data_docs_bucket - the bucket name for data docs only. When defined, it will
        supersede bucket parameter. Note: only applicable for store_backend s3.
    - expectations_store_prefix - prefix where to store expectations' data. Note: only
        applicable for store_backend s3.
    - validations_store_prefix - prefix where to store validations' data. Note: only
        applicable for store_backend s3.
    - data_docs_prefix - prefix where to store data_docs' data.
    - checkpoint_store_prefix - prefix where to store checkpoints' data. Note: only
        applicable for store_backend s3.
    - data_asset_name - name of the data asset to consider when configuring the great
        expectations' data source.
    - expectation_suite_name - name to consider for great expectations' suite.
    - result_sink_db_table - db.table_name indicating the database and table in which
        to save the results of the DQ process.
    - result_sink_location - file system location in which to save the results of the
        DQ process.
    - result_sink_partitions - the list of partitions to consider.
    - result_sink_format - format of the result table (e.g. delta, parquet, kafka...).
    - result_sink_options - extra spark options for configuring the result sink.
        E.g: can be used to configure a Kafka sink if result_sink_format is kafka.
    - result_sink_explode - flag to determine if the output table/location should have
        the columns exploded (as True) or not (as False). Default: True.
    - result_sink_extra_columns - list of extra columns to be exploded (following
        the pattern "<name>.*") or columns to be selected. It is only used when
        result_sink_explode is set to True.
    - source - name of data source, to be easier to identify in analysis. If not
        specified, it is set as default <input_id>. This will be only used
        when result_sink_explode is set to True.
    - fail_on_error - whether to fail the algorithm if the validations of your data in
        the DQ process failed.
    - cache_df - whether to cache the dataframe before running the DQ process or not.
    - critical_functions - functions that should not fail. When this argument is
        defined, fail_on_error is nullified.
    - max_percentage_failure - percentage of failure that should be allowed.
        This argument has priority over both fail_on_error and critical_functions.
    """

    spec_id: str
    input_id: str
    dq_type: str
    dq_functions: Optional[List[DQFunctionSpec]] = None
    unexpected_rows_pk: Optional[List[str]] = None
    tbl_to_derive_pk: Optional[str] = None
    gx_result_format: Optional[str] = "COMPLETE"
    tag_source_data: Optional[bool] = False
    store_backend: str = DQDefaults.STORE_BACKEND.value
    local_fs_root_dir: Optional[str] = None
    data_docs_local_fs: Optional[str] = None
    bucket: Optional[str] = None
    data_docs_bucket: Optional[str] = None
    expectations_store_prefix: str = DQDefaults.EXPECTATIONS_STORE_PREFIX.value
    validations_store_prefix: str = DQDefaults.VALIDATIONS_STORE_PREFIX.value
    data_docs_prefix: str = DQDefaults.DATA_DOCS_PREFIX.value
    checkpoint_store_prefix: str = DQDefaults.CHECKPOINT_STORE_PREFIX.value
    data_asset_name: Optional[str] = None
    expectation_suite_name: Optional[str] = None
    result_sink_db_table: Optional[str] = None
    result_sink_location: Optional[str] = None
    result_sink_partitions: Optional[List[str]] = None
    result_sink_format: str = OutputFormat.DELTAFILES.value
    result_sink_options: Optional[dict] = None
    result_sink_explode: bool = True
    result_sink_extra_columns: Optional[List[str]] = None
    source: Optional[str] = None
    fail_on_error: bool = True
    cache_df: bool = False
    critical_functions: Optional[List[DQFunctionSpec]] = None
    max_percentage_failure: Optional[float] = None


@dataclass
class TransformerSpec(object):
    """Transformer Specification, i.e., a single transformation amongst many.

    - function: name of the function (or callable function) to be executed.
    - args: (not applicable if using a callable function) dict with the arguments
        to pass to the function `<k,v>` pairs with the name of the parameter of
        the function and the respective value.
    """

    function: str
    args: dict


@dataclass
class OutputSpec(object):
    """Specification of an algorithm output.

    This is very aligned with the way the execution environment connects to the output
    systems (e.g., spark outputs).

    - spec_id: id of the output specification.
    - input_id: id of the corresponding input specification.
    - write_type: type of write operation.
    - data_format: format of the output. Defaults to DELTA.
    - db_table: table name in the form of `<db>.<table>`.
    - location: uri that identifies from where to write data in the specified format.
    - partitions: list of partition input_col names.
    - merge_opts: options to apply to the merge operation.
    - streaming_micro_batch_transformers: transformers to invoke for each streaming
        micro batch, before writing (i.e., in Spark's foreachBatch structured
        streaming function). Note: the lakehouse engine manages this for you, so
        you don't have to manually specify streaming transformations here, so we don't
        advise you to manually specify transformations through this parameter. Supply
        them as regular transformers in the transform_specs sections of an ACON.
    - streaming_once: if the streaming query is to be executed just once, or not,
        generating just one micro batch.
    - streaming_processing_time: if streaming query is to be kept alive, this indicates
        the processing time of each micro batch.
    - streaming_available_now: if set to True, set a trigger that processes all
        available data in multiple batches then terminates the query.
        When using streaming, this is the default trigger that the lakehouse-engine will
        use, unless you configure a different one.
    - streaming_continuous: set a trigger that runs a continuous query with a given
        checkpoint interval.
    - streaming_await_termination: whether to wait (True) for the termination of the
        streaming query (e.g. timeout or exception) or not (False). Default: True.
    - streaming_await_termination_timeout: a timeout to set to the
        streaming_await_termination. Default: None.
    - with_batch_id: whether to include the streaming batch id in the final data,
        or not. It only takes effect in streaming mode.
    - options: dict with other relevant options according to the execution environment
        (e.g., spark) possible outputs.  E.g.,: JDBC options, checkpoint location for
        streaming, etc.
    - streaming_micro_batch_dq_processors: similar to streaming_micro_batch_transformers
        but for the DQ functions to be executed. Used internally by the lakehouse
        engine, so you don't have to supply DQ functions through this parameter. Use the
        dq_specs of the acon instead.
    """

    spec_id: str
    input_id: str
    write_type: str
    data_format: str = OutputFormat.DELTAFILES.value
    db_table: Optional[str] = None
    location: Optional[str] = None
    merge_opts: Optional[MergeOptions] = None
    partitions: Optional[List[str]] = None
    streaming_micro_batch_transformers: Optional[List[TransformerSpec]] = None
    streaming_once: Optional[bool] = None
    streaming_processing_time: Optional[str] = None
    streaming_available_now: bool = True
    streaming_continuous: Optional[str] = None
    streaming_await_termination: bool = True
    streaming_await_termination_timeout: Optional[int] = None
    with_batch_id: bool = False
    options: Optional[dict] = None
    streaming_micro_batch_dq_processors: Optional[List[DQSpec]] = None


@dataclass
class TransformerSpec(object):
    """Transformer Specification, i.e., a single transformation amongst many.

    - function: name of the function (or callable function) to be executed.
    - args: (not applicable if using a callable function) dict with the arguments
        to pass to the function `<k,v>` pairs with the name of the parameter of
        the function and the respective value.
    """

    function: str
    args: dict


class SQLParser(Enum):
    """Defaults to use for parsing."""

    DOUBLE_QUOTES = '"'
    SINGLE_QUOTES = "'"
    BACKSLASH = "\\"
    SINGLE_TRACE = "-"
    DOUBLE_TRACES = "--"
    SLASH = "/"
    OPENING_MULTIPLE_LINE_COMMENT = "/*"
    CLOSING_MULTIPLE_LINE_COMMENT = "*/"
    PARAGRAPH = "\n"
    STAR = "*"

    MULTIPLE_LINE_COMMENT = [
        OPENING_MULTIPLE_LINE_COMMENT,
        CLOSING_MULTIPLE_LINE_COMMENT,
    ]


@dataclass
class DQFunctionSpec(object):
    """Defines a data quality function specification.

    - function - name of the data quality function (expectation) to execute.
    It follows the great_expectations api https://greatexpectations.io/expectations/.
    - args - args of the function (expectation). Follow the same api as above.
    """

    function: str
    args: Optional[dict] = None


@dataclass
class DQSpec(object):
    """Data quality overall specification.

    - spec_id - id of the specification.
    - input_id - id of the input specification.
    - dq_type - type of DQ process to execute (e.g. validator).
    - dq_functions - list of function specifications to execute.
    - unexpected_rows_pk - the list of columns composing the primary key of the
        source data to identify the rows failing the DQ validations. Note: only one
        of tbl_to_derive_pk or unexpected_rows_pk arguments need to be provided. It
        is mandatory to provide one of these arguments when using tag_source_data
        as True. When tag_source_data is False, this is not mandatory, but still
        recommended.
    - tbl_to_derive_pk - db.table to automatically derive the unexpected_rows_pk from.
        Note: only one of tbl_to_derive_pk or unexpected_rows_pk arguments need to
        be provided. It is mandatory to provide one of these arguments when using
        tag_source_data as True. hen tag_source_data is False, this is not
        mandatory, but still recommended.
    - gx_result_format - great expectations result format. Default: "COMPLETE".
    - tag_source_data - when set to true, this will ensure that the DQ process ends by
        tagging the source data with an additional column with information about the
        DQ results. This column makes it possible to identify if the DQ run was
        succeeded in general and, if not, it unlocks the insights to know what
        specific rows have made the DQ validations fail and why. Default: False.
        Note: it only works if result_sink_explode is True, gx_result_format is
        COMPLETE, fail_on_error is False (which is done automatically when
        you specify tag_source_data as True) and tbl_to_derive_pk or
        unexpected_rows_pk is configured.
    - store_backend - which store_backend to use (e.g. s3 or file_system).
    - local_fs_root_dir - path of the root directory. Note: only applicable for
        store_backend file_system.
    - data_docs_local_fs - the path for data docs only for store_backend
        file_system.
    - bucket - the bucket name to consider for the store_backend (store DQ artefacts).
        Note: only applicable for store_backend s3.
    - data_docs_bucket - the bucket name for data docs only. When defined, it will
        supersede bucket parameter. Note: only applicable for store_backend s3.
    - expectations_store_prefix - prefix where to store expectations' data. Note: only
        applicable for store_backend s3.
    - validations_store_prefix - prefix where to store validations' data. Note: only
        applicable for store_backend s3.
    - data_docs_prefix - prefix where to store data_docs' data.
    - checkpoint_store_prefix - prefix where to store checkpoints' data. Note: only
        applicable for store_backend s3.
    - data_asset_name - name of the data asset to consider when configuring the great
        expectations' data source.
    - expectation_suite_name - name to consider for great expectations' suite.
    - result_sink_db_table - db.table_name indicating the database and table in which
        to save the results of the DQ process.
    - result_sink_location - file system location in which to save the results of the
        DQ process.
    - result_sink_partitions - the list of partitions to consider.
    - result_sink_format - format of the result table (e.g. delta, parquet, kafka...).
    - result_sink_options - extra spark options for configuring the result sink.
        E.g: can be used to configure a Kafka sink if result_sink_format is kafka.
    - result_sink_explode - flag to determine if the output table/location should have
        the columns exploded (as True) or not (as False). Default: True.
    - result_sink_extra_columns - list of extra columns to be exploded (following
        the pattern "<name>.*") or columns to be selected. It is only used when
        result_sink_explode is set to True.
    - source - name of data source, to be easier to identify in analysis. If not
        specified, it is set as default <input_id>. This will be only used
        when result_sink_explode is set to True.
    - fail_on_error - whether to fail the algorithm if the validations of your data in
        the DQ process failed.
    - cache_df - whether to cache the dataframe before running the DQ process or not.
    - critical_functions - functions that should not fail. When this argument is
        defined, fail_on_error is nullified.
    - max_percentage_failure - percentage of failure that should be allowed.
        This argument has priority over both fail_on_error and critical_functions.
    """

    spec_id: str
    input_id: str
    dq_type: str
    dq_functions: Optional[List[DQFunctionSpec]] = None
    unexpected_rows_pk: Optional[List[str]] = None
    tbl_to_derive_pk: Optional[str] = None
    gx_result_format: Optional[str] = "COMPLETE"
    tag_source_data: Optional[bool] = False
    store_backend: str = DQDefaults.STORE_BACKEND.value
    local_fs_root_dir: Optional[str] = None
    data_docs_local_fs: Optional[str] = None
    bucket: Optional[str] = None
    data_docs_bucket: Optional[str] = None
    expectations_store_prefix: str = DQDefaults.EXPECTATIONS_STORE_PREFIX.value
    validations_store_prefix: str = DQDefaults.VALIDATIONS_STORE_PREFIX.value
    data_docs_prefix: str = DQDefaults.DATA_DOCS_PREFIX.value
    checkpoint_store_prefix: str = DQDefaults.CHECKPOINT_STORE_PREFIX.value
    data_asset_name: Optional[str] = None
    expectation_suite_name: Optional[str] = None
    result_sink_db_table: Optional[str] = None
    result_sink_location: Optional[str] = None
    result_sink_partitions: Optional[List[str]] = None
    result_sink_format: str = OutputFormat.DELTAFILES.value
    result_sink_options: Optional[dict] = None
    result_sink_explode: bool = True
    result_sink_extra_columns: Optional[List[str]] = None
    source: Optional[str] = None
    fail_on_error: bool = True
    cache_df: bool = False
    critical_functions: Optional[List[DQFunctionSpec]] = None
    max_percentage_failure: Optional[float] = None


@dataclass
class DQFunctionSpec(object):
    """Defines a data quality function specification.

    - function - name of the data quality function (expectation) to execute.
    It follows the great_expectations api https://greatexpectations.io/expectations/.
    - args - args of the function (expectation). Follow the same api as above.
    """

    function: str
    args: Optional[dict] = None
