import boto3
from botocore.exceptions import ClientError


class DynamoDBBackend:
    def __init__(self, table_name, region_name='us-east-1'):
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table_name = table_name

    def create_audit_table_if_not_exists(self):
        """Create audit table in DynamoDB if it does not exist."""
        try:
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[{'AttributeName': 'job_name', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'job_name', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            table.wait_until_exists()
            print(f"Audit table {self.table_name} created successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                print(f"Audit table {self.table_name} already exists.")
            else:
                raise

    def create_control_table_if_not_exists(self):
        """Create control table in DynamoDB if it does not exist."""
        control_table_name = f"{self.table_name}_control"
        try:
            table = self.dynamodb.create_table(
                TableName=control_table_name,
                KeySchema=[{'AttributeName': 'control_id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'control_id', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            table.wait_until_exists()
            print(f"Control table {control_table_name} created successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                print(f"Control table {control_table_name} already exists.")
            else:
                raise

    def insert_audit_record(self, item):
        """Insert a record into the DynamoDB audit table."""
        table = self.dynamodb.Table(self.table_name)
        table.put_item(Item=item)

    def insert_control_record(self, item):
        """Insert a record into the DynamoDB control table."""
        control_table_name = f"{self.table_name}_control"
        table = self.dynamodb.Table(control_table_name)
        table.put_item(Item=item)

    def update_control_record(self, control_id, update_expression, expression_attribute_values):
        """Update a record in the DynamoDB control table."""
        control_table_name = f"{self.table_name}_control"
        table = self.dynamodb.Table(control_table_name)
        table.update_item(
            Key={'control_id': control_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

    def delete_audit_record(self, job_name):
        """Delete a record from the DynamoDB audit table."""
        table = self.dynamodb.Table(self.table_name)
        table.delete_item(Key={'job_name': job_name})

    def delete_control_record(self, control_id):
        """Delete a record from the DynamoDB control table."""
        control_table_name = f"{self.table_name}_control"
        table = self.dynamodb.Table(control_table_name)
        table.delete_item(Key={'control_id': control_id})