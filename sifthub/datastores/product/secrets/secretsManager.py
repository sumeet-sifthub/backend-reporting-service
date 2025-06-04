from typing import Optional
import asyncio
import logging as logger
import boto3


class SecretsManager:

    def __init__(self, region_name: str):
        self.session = boto3.Session()
        self.region_name = region_name
        self._client = None

    def get_secret_string_sync(self, secret_name: str) -> Optional[str]:
        try:
            response = self.session.client(
                    service_name="secretsmanager",
                    region_name=self.region_name
            ).get_secret_value(SecretId=secret_name)
            return response.get('SecretString')
        except Exception as e:
            # Log error appropriately
            logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
            return None

    async def get_secret_string(self, secret_name: str) -> Optional[str]:
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._get_secret_sync, secret_name)
        except Exception as e:
            logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
            return None

    def _get_secret_sync(self, secret_name: str) -> Optional[str]:
        try:
            response = self.session.client(
                    service_name="secretsmanager",
                    region_name=self.region_name
            ).get_secret_value(SecretId=secret_name)
            return response.get('SecretString')
        except Exception as e:
            logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
            return None
