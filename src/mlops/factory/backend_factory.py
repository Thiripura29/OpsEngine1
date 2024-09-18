from mlops.sql.delta_backend import DeltaBackend
from mlops.sql.dynamodb_backend import DynamoDBBackend
from mlops.sql.sql_alchemy_backend import SQLAlchemyBackend


class BackendFactory:
    def __init__(self, spark=None):
        self.spark = spark

    def create_backend(self, strategy, format_config):
        """Factory method to create a backend instance based on the strategy."""
        if strategy == 'jdbc':
            return SQLAlchemyBackend(self.spark, format_config)
        elif strategy == 'dynamodb':
            return DynamoDBBackend(format_config)
        elif strategy == 'delta':
            return DeltaBackend(self.spark, format_config)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")