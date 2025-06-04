from typing import TypeVar
from google.cloud import firestore
from google.cloud.firestore_v1.async_client import AsyncClient
from sifthub.datastores.product.redis.user_role_access_cache import find_role_mapping_by_user_id
from sifthub.utils import stringUtil
from sifthub.utils.logger import setup_logger

logger = setup_logger()
T = TypeVar('T')


class FirebasePublisher:

    def __init__(self, firestore_client: AsyncClient):
        self.firestore: AsyncClient = firestore_client

    async def publish_at_user(self, data: T, collection: str, event_id: str,
                             user_id: int, client_id: int, product_id: int) -> bool:
        try:
            user_role_data = await find_role_mapping_by_user_id(user_id, client_id, product_id)
            if user_role_data is None:
                logger.warning(f"User role data not found for client_id: {client_id}, product_id: {product_id}, user_id: {user_id}")
                return False

            # Use event_id if provided, otherwise use user_guid
            event_id = user_role_data.user_guid if await stringUtil.empty(event_id) else event_id

            # Create the event reference using path building
            event_ref = self.firestore.collection("pd").document(user_role_data.get('productGuid')) \
                .collection("cl").document(user_role_data.get('clientGuid')) \
                .collection("usr").document(user_role_data.get('userGuid')) \
                .collection(collection).document(event_id)

            # Async set operation
            await event_ref.set(data)
            logger.info(f"Successfully published data to Firebase for client_id: {client_id}, product_id: {product_id}, user_id: {user_id}, event_id: {event_id}")
            return True

        except Exception as ex:
            logger.error(f"Failed to publish in firebase for id: {event_id}. Error Message: {str(ex)}", exc_info=True)
            return False

    async def publish_export_notification(self, event_id: str, download_url: str, status: str,
                                        user_id: int, client_id: int, product_id: int) -> bool:
        notification_data = {
            "eventId": event_id,
            "type": "EXPORT_COMPLETE",
            "status": status,
            "downloadUrl": download_url,
            "timestamp": firestore.SERVER_TIMESTAMP,
            "message": f"Your export is ready for download" if status == "SUCCESS" else "Export failed"
        }
        
        return await self.publish_at_user(
            data=notification_data,
            collection="notifications",
            event_id=event_id,
            user_id=user_id,
            client_id=client_id,
            product_id=product_id
        ) 