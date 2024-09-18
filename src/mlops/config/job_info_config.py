from mlops.config.local_config import MockDBUtils


class JobInfoConfig:
    def __init__(self, platform):
        self.job_id = ""
        self.job_name = ""

        if platform == "local":
            self.set_local_job_info()
        elif platform == "databricks":
            self.set_databricks_job_info()

    def set_databricks_job_info(self):
        import dbutils
        # Get the context of the currently running notebook
        context = dbutils.notebook.getContext()

        # Retrieve job ID and job name
        self.job_id = context.jobId().get()
        self.job_name = context.jobName().get()

    def set_local_job_info(self):
        # Create an instance of the mock dbutils
        dbutils = MockDBUtils()

        # Retrieve job ID and job name using the mock
        self.job_id = dbutils.notebook().getContext().jobId().get()
        self.job_name = dbutils.notebook().getContext().jobName().get()
