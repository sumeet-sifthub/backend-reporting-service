from typing import Optional

from firebase_admin import App, credentials, initialize_app
from google.cloud.firestore_v1 import AsyncClient
from google.cloud import firestore
import json

from sifthub.configs import aws_configs
from sifthub.datastores.product.firebase.firebase_publisher import FirebasePublisher
from sifthub.datastores.product.secrets.secretsManager import SecretsManager


class Firebase:
    _app: Optional[App] = None
    _firestore: Optional[AsyncClient] = None
    _publisher: Optional[FirebasePublisher] = None

    @classmethod
    def initialize(cls) -> None:
        if cls._app is None:
            # Get Firebase credentials from secrets using sync method
            secrets_manager = SecretsManager(region_name=aws_configs.AWS_REGION)
            service_account_json = secrets_manager.get_secret_string_sync("notifications/internal/FIREBASE")
            
            if service_account_json is None:
                raise RuntimeError("Failed to retrieve Firebase credentials from secrets manager")
                
            cred_dict = json.loads(service_account_json)

            # Initialize Firebase app
            cred = credentials.Certificate(cred_dict)
            cls._app = initialize_app(cred)

            # Initialize Firestore client
            cls._firestore = firestore.AsyncClient(
                project=cred_dict['project_id'],
                credentials=cred.get_credential()
            )

            # Initialize publisher
            cls._publisher = FirebasePublisher(cls._firestore)

    @classmethod
    def get_app(cls) -> App:
        if cls._app is None:
            cls.initialize()
        return cls._app

    @classmethod
    def get_firestore(cls) -> AsyncClient:
        if cls._firestore is None:
            cls.initialize()
        return cls._firestore

    @classmethod
    def get_publisher(cls) -> FirebasePublisher:
        if cls._publisher is None:
            cls.initialize()
        return cls._publisher
