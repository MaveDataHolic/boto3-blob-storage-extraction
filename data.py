import boto3

class AzureToS3DataTransfer:
    def __init__(self, azure_access_key, azure_secret_key, azure_container, s3_bucket):
        self.azure_blob_service = boto3.client(
            's3',
            aws_access_key_id=azure_access_key,
            aws_secret_access_key=azure_secret_key,
            endpoint_url='https://YOUR_AZURE_STORAGE_ACCOUNT.blob.core.windows.net'
        )
        self.azure_container = azure_container
        self.s3_bucket = s3_bucket

    def list_azure_blobs(self):
        marker = None
        while True:
            batch = self.azure_blob_service.list_blobs(self.azure_container, marker=marker)
            for blob in batch:
                yield blob.name
            if not batch.next_marker:
                break
            marker = batch.next_marker

    def download_and_upload(self, blob_name):
        current_blob = self.azure_blob_service.get_blob_to_bytes(self.azure_container, blob_name)
        # Upload the blob content to S3
        s3_client.put_object(
            Body=current_blob.content,
            Bucket=self.s3_bucket,
            ContentType=current_blob.properties.content_settings.content_type,
            Key=blob_name
        )

if __name__ == "__main__":
    azure_access_key = 'YOUR_AZURE_ACCESS_KEY'
    azure_secret_key = 'YOUR_AZURE_SECRET_KEY'
    azure_container = 'your_azure_container'
    s3_bucket = 'your_s3_bucket'

    transfer = AzureToS3DataTransfer(azure_access_key, azure_secret_key, azure_container, s3_bucket)

    for blob_name in transfer.list_azure_blobs():
        transfer.download_and_upload(blob_name)
