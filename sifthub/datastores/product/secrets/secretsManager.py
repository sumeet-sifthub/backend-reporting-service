from typing import Optional

import logging as logger
import boto3


class SecretsManager:

    def __init__(self, region_name: str):
        self.session = boto3.Session()
        self.region_name = region_name
        self._client = None

    def get_secret_string(self, secret_name: str) -> Optional[str]:
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
