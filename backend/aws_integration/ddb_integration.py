import csv
import boto3
import gzip
import io

# from botocore.exceptions import ClientError


class S3Service:
    def __init__(self) -> None:
        self.s3_client = boto3.client("s3")

    def list_objects_by_prefix(self, bucket: str, prefix: str) -> list:
        """
        Lists objects in an S3 bucket with a given prefix.

        Args:
            bucket (str): The name of the S3 bucket.
            prefix (str): The prefix to filter the objects by.

        Returns:
            list: A list of objects matching the given prefix.
        """
        s3_results = self.s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
        )
        if s3_results["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return s3_results["Contents"]

    def read_object_from_s3(self, bucket_name: str, key: str) -> bytes:
        """
        Reads an object from an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            key (str): The key of the object to read.

        Returns:
            bytes: The content of the object as bytes.
        """
        obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        return obj["Body"].read()

    def read_gzip_object_from_s3(self, bucket: str, key: str):
        """
        Reads a GZIP object from an S3 bucket.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The key of the object in the S3 bucket.

        Returns:
            str: The contents of the GZIP object, decoded as UTF-8.
        """
        returned_body = self.s3_client.get_object(Bucket=bucket, Key=key)
        bytes_stream = io.BytesIO(returned_body["Body"].read())
        return gzip.GzipFile(None, "rb", fileobj=bytes_stream).read().decode("utf-8")

    def write_file_to_s3(
        self, file_content: str, s3_bucket: str, file_key: str, content_type: str
    ) -> None:
        """
        Writes a file to an S3 bucket.

        Args:
            file_content (str): The content of the file to be written.
            s3_bucket (str): The name of the S3 bucket.
            file_key (str): The key (path) of the file in the S3 bucket.
            content_type (str): The content type of the file.

        Raises:
            Exception: If there is an error while writing the file to S3.
        """
        try:
            self.s3_client.put_object(
                Bucket=s3_bucket,
                Key=file_key,
                Body=file_content,
                ContentType=content_type,
            )
        except Exception as e:
            raise e

    def delete_object_from_s3(self, bucket: str, key: str) -> str:
        """
        Deletes an object from the specified S3 bucket.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The key of the object to delete.

        Returns:
            str: The response from the S3 service.

        """
        response = self.s3_client.delete_object(Bucket=bucket, Key=key)
        return response

    def rename_file(self, bucket: str, old_file_key: str, new_file_key: str) -> None:
        """
        Renames a file in the specified S3 bucket.

        Args:
            bucket (str): The name of the S3 bucket.
            old_file_key (str): The key of the file to be renamed.
            new_file_key (str): The new key for the renamed file.

        Raises:
            Exception: If an error occurs during the renaming process.
        """
        try:
            self.s3_client.copy_object(
                Bucket=bucket,
                Key=new_file_key,
                CopySource={"Bucket": bucket, "Key": old_file_key},
            )

            self.s3_client.delete_object(Bucket=bucket, Key=old_file_key)
        except Exception as e:
            raise e

    def read_multiple_csv_from_s3(
        self, s3_bucket: str, prefix: str, columns: list
    ) -> list:
        """
        Reads multiple CSV files from an S3 bucket.

        Args:
            s3_bucket (str): The name of the S3 bucket.
            prefix (str): The prefix of the objects in the S3 bucket.
            columns (list): The list of column names for the CSV files.

        Returns:
            list: A list of dictionaries representing the parsed data from the CSV files.

        Raises:
            Exception: If there is an error while reading the CSV files from S3.
        """
        try:
            res = self.s3_client.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=prefix,
            )
            final_data_list = []
            if res["ResponseMetadata"]["HTTPStatusCode"] == 200:
                for content in res["Contents"]:
                    # Get object from S3
                    object_key = content["Key"]
                    s3_object = self.s3_client.get_object(
                        Bucket=s3_bucket, Key=object_key
                    )
                    # Parse csv data
                    csv_data = s3_object["Body"].read().decode("utf-8")
                    csv_data_list = csv_data.splitlines()
                    csv_data_list = csv_data_list[1:]
                    # Convert to list of dict to be saved by bulk_save_data
                    dictionary_reader = csv.DictReader(
                        csv_data_list, fieldnames=columns
                    )
                    parsed_data_list = list(dictionary_reader)
                    final_data_list = final_data_list + parsed_data_list
            return final_data_list
        except Exception as e:
            raise e

    def write_gzip_to_s3(
        self, file_content: str, s3_bucket: str, file_key: str, content_type: str
    ) -> None:
        """
        Writes the given file content as a gzipped file to the specified S3 bucket.

        Args:
            file_content (str): The content of the file to be written.
            s3_bucket (str): The name of the S3 bucket to write the file to.
            file_key (str): The key (path) of the file within the S3 bucket.
            content_type (str): The content type of the file.

        Raises:
            Exception: If an error occurs while writing the file to S3.

        Returns:
            None
        """
        file_name = file_key.split("/")[-1]
        try:
            zip_buffer = io.BytesIO()
            with gzip.GzipFile(
                filename=file_name, mode="wb", fileobj=zip_buffer
            ) as zipper:
                zipper.write(file_content.encode("utf-8"))

            self.s3_client.put_object(
                Bucket=s3_bucket,
                Key=file_key,
                Body=zip_buffer.getvalue(),
                ContentEncoding="gzip",
                ContentType=content_type,
            )
        except Exception as e:
            raise e

    def write_zip_to_s3(self, file_path: str, s3_bucket: str, file_key: str) -> None:
        """
        Writes a zip file to an S3 bucket. (Uses local filesystem)

        Args:
            file_path (str): The local file path of the zip file.
            s3_bucket (str): The name of the S3 bucket.
            file_key (str): The key (path) of the file in the S3 bucket.

        Raises:
            Exception: If there is an error during the upload process.
        """
        try:
            with open(file_path, "rb") as zip_file:
                self.s3_client.upload_fileobj(zip_file, s3_bucket, file_key)
        except Exception as e:
            raise e
