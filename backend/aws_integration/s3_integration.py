import time
import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError


class DynamoDbService:
    def __init__(self) -> None:
        self.dynamodb_client = boto3.client("dynamodb")
        self.dynamodb_resource = boto3.resource("dynamodb")

    def __python_obj_to_dynamo_obj(self, python_obj: dict) -> dict:
        """
        Converts a Python object to a DynamoDB object.

        Args:
            python_obj (dict): The Python object to be converted.

        Returns:
            dict: The converted DynamoDB object.
        """
        serializer = TypeSerializer()
        return {k: serializer.serialize(v) for k, v in python_obj.items()}

    def query_table(
        self,
        table_name: str,
        query_kwargs: dict,
    ) -> list:
        """
        Queries a DynamoDB table and retrieves the items based on the provided query parameters.

        Args:
            table_name (str): The name of the DynamoDB table to query.
            query_kwargs (dict): The query parameters to be passed to the `query` method.

        Returns:
            list: A list of retrieved items from the DynamoDB table.

        Raises:
            ClientError: If there is an error while querying for data.
        """
        table = self.dynamodb_resource.Table(table_name)
        retrieved = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    query_kwargs["ExclusiveStartKey"] = start_key
                response = table.query(**query_kwargs)
                retrieved.extend(response.get("Items", []))
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except ClientError as e:
            raise e
        else:
            return retrieved

    def scan_table(self, table_name: str, scan_kwargs: dict) -> list:
        """
        Scans a DynamoDB table and retrieves all items that match the provided scan criteria.

        Args:
            table_name (str): The name of the DynamoDB table to scan.
            scan_kwargs (dict): Additional parameters to customize the scan operation.

        Returns:
            list: A list of items retrieved from the DynamoDB table.

        Raises:
            ClientError: If there is an error while scanning the table.

        """
        table = self.dynamodb_resource.Table(table_name)
        retrieved = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = table.scan(**scan_kwargs)
                retrieved.extend(response.get("Items", []))
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except ClientError as e:
            raise e
        else:
            return retrieved

    def batch_get(self, batch_keys: dict):
        """
        Gets a batch of items from Amazon DynamoDB. Batches can contain keys from
        more than one table.

        When Amazon DynamoDB cannot process all items in a batch, a set of unprocessed
        keys is returned. This function uses an exponential backoff algorithm to retry
        getting the unprocessed keys until all are retrieved or the specified
        number of tries is reached.

        Args:
            batch_keys: The set of keys to retrieve. A batch can contain at most 100 keys. Otherwise, Amazon DynamoDB returns an error.

        Returns:
            list: The dictionary of retrieved items grouped under their respective table names.
        """
        tries = 0
        max_tries = 10
        sleepy_time = 1  # Start with 1 second of sleep, then exponentially increase.
        retrieved = {key: [] for key in batch_keys}
        while tries < max_tries:
            response = self.dynamodb_resource.batch_get_item(RequestItems=batch_keys)
            # Collect any retrieved items and retry unprocessed keys.
            for key in response.get("Responses", []):
                retrieved[key] += response["Responses"][key]
            unprocessed = response["UnprocessedKeys"]
            if len(unprocessed) > 0:
                batch_keys = unprocessed
                tries += 1
                if tries < max_tries:
                    time.sleep(sleepy_time)
                    sleepy_time = min(sleepy_time * 2, 32)
            else:
                break
        return retrieved

    def batch_upsert(self, records: list, table_name: str):
        """
        Upserts a batch of records into a DynamoDB table.

        Args:
            records (list): A list of records to be upserted.
            table_name (str): The name of the DynamoDB table.

        Raises:
            Exception: If there is an error when writing the batch to the DynamoDB table.
        """
        put_requests = []
        for item in records:
            item = self.__python_obj_to_dynamo_obj(item)
            put_requests.append({"PutRequest": {"Item": item}})
        # SPLIT REOCRDS IN BATCHES OF 25
        put_batches = [
            put_requests[i : i + 25] for i in range(0, len(put_requests), 25)  # noqa
        ]
        for put_batch in put_batches:
            response = self.dynamodb_client.batch_write_item(
                RequestItems={table_name: put_batch}
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise Exception(  # pylint: disable=broad-exception-raised
                    f"Error when writing batch to dynamo table {table_name}: {response}"
                )

    def update_item(self, table_name: str, update_kwargs: dict):
        """
        Updates an item in the specified DynamoDB table.

        Args:
            table_name (str): The name of the DynamoDB table.
            update_kwargs (dict): The update arguments to be passed to the `update_item` method.

        Raises:
            Exception: If there is an error when updating the item in DynamoDB.

        Returns:
            None
        """
        table = self.dynamodb_resource.Table(table_name)
        response = table.update_item(**update_kwargs)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception(  # pylint: disable=broad-exception-raised
                f"Error when updating item in dynamo table {table_name}: {response}"
            )

    def transaction_write_items(self, table_name: str, records: list):
        """
        Writes multiple items to a DynamoDB table using a transaction.

        Args:
            table_name (str): The name of the DynamoDB table.
            records (list): A list of records to be written to the table.

        Raises:
            Exception: If there is an error when writing items to the DynamoDB table.

        Returns:
            None
        """
        batch_size = 100
        batches = [
            records[i : i + batch_size]  # noqa
            for i in range(0, len(records), batch_size)
        ]
        for batch in batches:
            response = self.dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": table_name,
                            "Item": self.__python_obj_to_dynamo_obj(record),
                        }
                    }
                    for record in batch
                ]
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise Exception(  # pylint: disable=broad-exception-raised
                    f"Error when writing items to dynamo table {table_name}: {response}"
                )
