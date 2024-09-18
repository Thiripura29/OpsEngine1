from collections import OrderedDict
from typing import List, Tuple

from mlops.core.definitions import DQSpec, DQFunctionSpec, DQDefaults, OutputFormat
from mlops.dq_processors.dq_factory import DQFactory
from mlops.logging.ops_logger import OpsLogger
from pyspark.sql import SparkSession


class DQLoader(object):
    def __init__(self, config):
        self.dq_config = config
        self.dq_specs: List[DQSpec] = self._get_dq_specs()
        # self._logger = OpsLogger(__name__).get_logger()

    @classmethod
    def get_dq_spec(
            cls, spec: dict
    ) -> Tuple[DQSpec, List[DQFunctionSpec], List[DQFunctionSpec]]:
        """Get data quality specification object from acon.

        Args:
            spec: data quality specifications.

        Returns:
            The DQSpec and the List of DQ Functions Specs.
        """
        input_id = spec.get("input_id", None)
        source = input_id if input_id else spec["dq_name"]
        dq_spec = DQSpec(
            spec_id=spec["dq_name"],
            input_id=input_id,
            dq_type=spec["dq_type"],
            dq_functions=[],
            unexpected_rows_pk=spec.get(
                "unexpected_rows_pk", DQSpec.unexpected_rows_pk
            ),
            gx_result_format=spec.get("gx_result_format", DQSpec.gx_result_format),
            tbl_to_derive_pk=spec.get("tbl_to_derive_pk", DQSpec.tbl_to_derive_pk),
            tag_source_data=spec.get("tag_source_data", DQSpec.tag_source_data),
            data_asset_name=spec.get("data_asset_name", DQSpec.data_asset_name),
            expectation_suite_name=spec.get(
                "expectation_suite_name", DQSpec.expectation_suite_name
            ),
            store_backend=spec.get("store_backend", DQDefaults.STORE_BACKEND.value),
            local_fs_root_dir=spec.get("local_fs_root_dir", DQSpec.local_fs_root_dir),
            data_docs_local_fs=spec.get(
                "data_docs_local_fs", DQSpec.data_docs_local_fs
            ),
            bucket=spec.get("bucket", DQSpec.bucket),
            data_docs_bucket=spec.get("data_docs_bucket", DQSpec.data_docs_bucket),
            checkpoint_store_prefix=spec.get(
                "checkpoint_store_prefix", DQDefaults.CHECKPOINT_STORE_PREFIX.value
            ),
            expectations_store_prefix=spec.get(
                "expectations_store_prefix",
                DQDefaults.EXPECTATIONS_STORE_PREFIX.value,
            ),
            data_docs_prefix=spec.get(
                "data_docs_prefix", DQDefaults.DATA_DOCS_PREFIX.value
            ),
            validations_store_prefix=spec.get(
                "validations_store_prefix",
                DQDefaults.VALIDATIONS_STORE_PREFIX.value,
            ),
            result_sink_db_table=spec.get(
                "result_sink_db_table", DQSpec.result_sink_db_table
            ),
            result_sink_location=spec.get(
                "result_sink_location", DQSpec.result_sink_location
            ),
            result_sink_partitions=spec.get(
                "result_sink_partitions", DQSpec.result_sink_partitions
            ),
            result_sink_format=spec.get(
                "result_sink_format", OutputFormat.DELTAFILES.value
            ),
            result_sink_options=spec.get(
                "result_sink_options", DQSpec.result_sink_options
            ),
            result_sink_explode=spec.get(
                "result_sink_explode", DQSpec.result_sink_explode
            ),
            result_sink_extra_columns=spec.get("result_sink_extra_columns", []),
            source=spec.get("source", source),
            fail_on_error=spec.get("fail_on_error", DQSpec.fail_on_error),
            cache_df=spec.get("cache_df", DQSpec.cache_df),
            critical_functions=spec.get(
                "critical_functions", DQSpec.critical_functions
            ),
            max_percentage_failure=spec.get(
                "max_percentage_failure", DQSpec.max_percentage_failure
            ),
        )

        dq_functions = cls._get_dq_functions(spec, "dq_functions")

        critical_functions = cls._get_dq_functions(spec, "critical_functions")

        cls._validate_dq_tag_strategy(dq_spec)

        return dq_spec, dq_functions, critical_functions

    @staticmethod
    def _get_dq_functions(spec: dict, function_key: str) -> List[DQFunctionSpec]:
        """Get DQ Functions from a DQ Spec, based on a function_key.

        Args:
            spec: data quality specifications.
            function_key: dq function key ("dq_functions" or
                "critical_functions").

        Returns:
            a list of DQ Function Specs.
        """
        functions = []

        if spec.get(function_key, []):
            for f in spec.get(function_key, []):
                dq_fn_spec = DQFunctionSpec(
                    function=f["function"],
                    args=f.get("args", {}),
                )
                functions.append(dq_fn_spec)

        return functions

    @staticmethod
    def _validate_dq_tag_strategy(spec: DQSpec) -> None:
        """Validate DQ Spec arguments related with the data tagging strategy.

        Args:
            spec: data quality specifications.
        """
        if spec.tag_source_data:
            spec.gx_result_format = DQSpec.gx_result_format
            spec.fail_on_error = False
            spec.result_sink_explode = DQSpec.result_sink_explode
        elif spec.gx_result_format != DQSpec.gx_result_format:
            spec.tag_source_data = False

    def _get_dq_specs(self) -> List[DQSpec]:
        """Get list of data quality specification objects from acon.

        In streaming mode, we automatically convert the data quality specification in
        the streaming_micro_batch_dq_processors list for the respective output spec.
        This is needed because our dq process cannot be executed using native streaming
        functions.

        Returns:
            List of data quality spec objects.
        """

        dq_specs = []
        for spec in self.dq_config.get("dq_specs", []):
            dq_spec, dq_functions, critical_functions = DQLoader.get_dq_spec(spec)

            dq_spec.dq_functions = dq_functions
            dq_spec.critical_functions = critical_functions

            dq_specs.append(dq_spec)

        return dq_specs

    def process_dq(self, spark: SparkSession,data: OrderedDict) -> OrderedDict:
        """Process the data quality tasks for the data that was read and/or transformed.

        It supports multiple input dataframes. Although just one is advisable.

        It is possible to use data quality validators/expectations that will validate
        your data and fail the process in case the expectations are not met. The DQ
        process also generates and keeps updating a site containing the results of the
        expectations that were done on your data. The location of the site is
        configurable and can either be on file system or S3. If you define it to be
        stored on S3, you can even configure your S3 bucket to serve the site so that
        people can easily check the quality of your data. Moreover, it is also
        possible to store the result of the DQ process into a defined result sink.

        Args:
            data: dataframes from previous steps of the algorithm that we which to
                run the DQ process on.

        Returns:
            Another ordered dict with the validated dataframes.
        """
        if not self.dq_specs:
            return data
        else:

            dq_processed_dfs = OrderedDict(data)
            for spec in self.dq_specs:

                dq_processed_name = spec.input_id if spec.input_id else spec.spec_id
                df_processed_df = dq_processed_dfs[dq_processed_name]
                # self._logger.info(f"Found data quality specification: {spec}")
                if spec.cache_df:
                    df_processed_df.cache()
                dq_processed_dfs[spec.spec_id] = DQFactory.run_dq_process(spark,
                    spec, df_processed_df)
            return dq_processed_dfs
