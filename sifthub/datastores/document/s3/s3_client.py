import aioboto3
from io import BytesIO
from typing import Optional
from datetime import datetime
from sifthub.configs import aws_configs
from sifthub.configs.constants import EXPORT_FILE_EXPIRY_HOURS
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)

_session = aioboto3.Session()


class S3Client:
    
    @property
    def bucket_name(self) -> str:
        return aws_configs.AWS_S3_BUCKET
    
    async def upload_file(self, file_stream: BytesIO, key: str, content_type: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") -> bool:
        return await self.upload_file_stream(file_stream, key, content_type)
    
    async def upload_file_stream(self, file_stream: BytesIO, key: str, content_type: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") -> bool:
        try:
            async with _session.client(
                's3',
                region_name=aws_configs.AWS_REGION,
                aws_access_key_id=aws_configs.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=aws_configs.AWS_SECRET_ACCESS_KEY
            ) as s3_client:
                file_size = len(file_stream.getvalue())
                logger.info(f"Uploading file to S3: {key}, size: {file_size} bytes")
                
                # For files larger than 5MB, use multipart upload
                if file_size > 5 * 1024 * 1024:  # 5MB
                    await self._multipart_upload(s3_client, file_stream, key, content_type)
                else:
                    # Simple upload for smaller files
                    file_stream.seek(0)
                    await s3_client.put_object(
                        Bucket=aws_configs.AWS_S3_BUCKET,
                        Key=key,
                        Body=file_stream.getvalue(),
                        ContentType=content_type
                    )
                logger.info(f"Successfully uploaded file to S3: {key}")
                return True
        except Exception as e:
            logger.error(f"Failed to upload file to S3: {e}", exc_info=True)
            return False

    async def download_file(self, key: str) -> Optional[BytesIO]:
        try:
            async with _session.client(
                's3',
                region_name=aws_configs.AWS_REGION,
                aws_access_key_id=aws_configs.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=aws_configs.AWS_SECRET_ACCESS_KEY
            ) as s3_client:
                logger.info(f"Downloading file from S3: {key}")
                response = await s3_client.get_object(
                    Bucket=aws_configs.AWS_S3_BUCKET,
                    Key=key
                )
                content = await response['Body'].read()
                file_stream = BytesIO(content)
                file_stream.seek(0)
                logger.info(f"Successfully downloaded file from S3: {key}, size: {len(content)} bytes")
                return file_stream
        except Exception as e:
            logger.error(f"Failed to download file from S3: {e}", exc_info=True)
            return None

    async def _multipart_upload(self, s3_client, file_stream: BytesIO, key: str, content_type: str):
        upload_id = None
        try:
            response = await s3_client.create_multipart_upload(
                Bucket=aws_configs.AWS_S3_BUCKET,
                Key=key,
                ContentType=content_type
            )
            upload_id = response['UploadId']
            part_size = 5 * 1024 * 1024  # 5MB per part
            file_stream.seek(0)
            parts = []
            part_number = 1
            while True:
                chunk = file_stream.read(part_size)
                if not chunk:
                    break
                part_response = await s3_client.upload_part(
                    Bucket=aws_configs.AWS_S3_BUCKET,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )

                parts.append({
                    'ETag': part_response['ETag'],
                    'PartNumber': part_number
                })
                part_number += 1
            # Complete multipart upload
            await s3_client.complete_multipart_upload(
                Bucket=aws_configs.AWS_S3_BUCKET,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            logger.info(f"Multipart upload completed for {key}")
        except Exception as e:
            # Abort multipart upload on error
            if upload_id:
                try:
                    await s3_client.abort_multipart_upload(
                        Bucket=aws_configs.AWS_S3_BUCKET,
                        Key=key,
                        UploadId=upload_id
                    )
                except:
                    pass
            raise e

    async def generate_presigned_url(self, key: str, expiration_hours: int = EXPORT_FILE_EXPIRY_HOURS) -> Optional[str]:
        try:
            async with _session.client(
                's3',
                region_name=aws_configs.AWS_REGION,
                aws_access_key_id=aws_configs.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=aws_configs.AWS_SECRET_ACCESS_KEY
            ) as s3_client:
                url = await s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': aws_configs.AWS_S3_BUCKET, 'Key': key},
                    ExpiresIn=expiration_hours * 3600  # Convert hours to seconds
                )
                logger.info(f"Generated presigned URL for {key}, expires in {expiration_hours} hours")
                return url
        except Exception as e:
            logger.error(f"Failed to generate presigned URL for {key}: {e}", exc_info=True)
            return None

    def generate_s3_key(self, event_id: str, client_id: str, module: str, export_type: str, sub_type: str) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{module}_{export_type}_{sub_type}_{timestamp}.xlsx"
        return f"exports/{client_id}/{event_id}/{filename}"