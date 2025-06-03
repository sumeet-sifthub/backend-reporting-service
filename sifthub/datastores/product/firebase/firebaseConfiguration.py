from firebase_admin import credentials, initialize_app, App
import json
from typing import Optional

from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient

from sifthub.datastores.product.secrets.secretsManager import SecretsManager


class FirebaseConfiguration:
    FIREBASE_SVC_SECRETS = "notifications/internal/FIREBASE"

    def __init__(self, secrets_manager: SecretsManager):
        self._firebase_app: Optional[App] = None
        self._firebase_client: Optional[AsyncClient] = None
        self._secret_manager = secrets_manager

    async def initialize_firebase_app(self) -> [App, AsyncClient]:
        if self._firebase_app is None:
            service_account_json = await self._secret_manager.get_secret_string(self.FIREBASE_SVC_SECRETS)
            cred_dict = json.loads(service_account_json)
            cred = credentials.Certificate(cred_dict)
            self._firebase_app = initialize_app(cred)
            self._firebase_client = firestore.AsyncClient(
                project=cred_dict['project_id'],
                credentials=cred.get_credential()
            )
        return self._firebase_app, self._firebase_client
