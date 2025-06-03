import asyncio
import json
import signal
import sys
from typing import Optional
import aioboto3
from botocore.exceptions import ClientError, NoCredentialsError
from sifthub.reporting.models.export_models import SQSExportMessage
from sifthub.reporting.services.export_service import export_service
from sifthub.configs.app_config import app_config
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class SQSConsumer:
    """SQS Consumer for processing export requests"""

    def __init__(self):
        self.session = None
        self.sqs_client = None
        self.queue_url = app_config.SQS_QUEUE_URL
        self.running = False
        self.max_messages = 10
        self.wait_time_seconds = 20
        self.visibility_timeout = 300  # 5 minutes

    async def initialize(self):
        """Initialize SQS client and export service"""
        try:
            # Initialize aioboto3 session
            self.session = aioboto3.Session()
            self.sqs_client = self.session.client(
                'sqs',
                region_name=app_config.AWS_REGION,
                aws_access_key_id=app_config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=app_config.AWS_SECRET_ACCESS_KEY
            )
            
            # Initialize export service
            await export_service.initialize()
            
            logger.info("SQS Consumer initialized successfully")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize SQS consumer: {e}", exc_info=True)
            raise

    async def start_consuming(self):
        """Start consuming messages from SQS"""
        self.running = True
        logger.info(f"Starting SQS consumer for queue: {self.queue_url}")
        
        try:
            async with self.sqs_client as sqs:
                while self.running:
                    try:
                        # Receive messages from SQS
                        response = await sqs.receive_message(
                            QueueUrl=self.queue_url,
                            MaxNumberOfMessages=self.max_messages,
                            WaitTimeSeconds=self.wait_time_seconds,
                            VisibilityTimeout=self.visibility_timeout,
                            MessageAttributeNames=['All']
                        )
                        
                        messages = response.get('Messages', [])
                        if not messages:
                            logger.debug("No messages received from SQS")
                            continue
                        
                        logger.info(f"Received {len(messages)} messages from SQS")
                        
                        # Process messages concurrently
                        tasks = []
                        for message in messages:
                            task = asyncio.create_task(
                                self._process_message(sqs, message)
                            )
                            tasks.append(task)
                        
                        # Wait for all tasks to complete
                        await asyncio.gather(*tasks, return_exceptions=True)
                        
                    except ClientError as e:
                        logger.error(f"AWS SQS error: {e}", exc_info=True)
                        await asyncio.sleep(5)  # Wait before retrying
                        
                    except Exception as e:
                        logger.error(f"Unexpected error in consumer loop: {e}", exc_info=True)
                        await asyncio.sleep(5)  # Wait before retrying
                        
        except Exception as e:
            logger.error(f"Fatal error in SQS consumer: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()

    async def _process_message(self, sqs_client, message: dict):
        """Process a single SQS message"""
        receipt_handle = message.get('ReceiptHandle')
        message_id = message.get('MessageId')
        
        try:
            logger.info(f"Processing message: {message_id}")
            
            # Parse message body
            body = json.loads(message.get('Body', '{}'))
            
            # Create SQS export message object
            export_message = SQSExportMessage.from_dict(body)
            
            # Validate message
            if not self._validate_message(export_message):
                logger.error(f"Invalid message format: {message_id}")
                await self._delete_message(sqs_client, receipt_handle)
                return
            
            # Process export request
            success = await export_service.process_export_request(export_message)
            
            if success:
                logger.info(f"Successfully processed message: {message_id}")
                # Delete message from queue
                await self._delete_message(sqs_client, receipt_handle)
            else:
                logger.error(f"Failed to process message: {message_id}")
                # Message will become visible again after visibility timeout
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message {message_id}: {e}")
            await self._delete_message(sqs_client, receipt_handle)
            
        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}", exc_info=True)
            # Message will become visible again after visibility timeout

    async def _delete_message(self, sqs_client, receipt_handle: str):
        """Delete message from SQS queue"""
        try:
            await sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.debug("Message deleted from queue")
            
        except Exception as e:
            logger.error(f"Failed to delete message from queue: {e}")

    def _validate_message(self, message: SQSExportMessage) -> bool:
        """Validate SQS export message"""
        try:
            required_fields = ['eventId', 'clientId', 'user_id', 'module', 'type', 'mode']
            
            for field in required_fields:
                if not hasattr(message, field) or getattr(message, field) is None:
                    logger.error(f"Missing required field: {field}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating message: {e}")
            return False

    def stop_consuming(self):
        """Stop consuming messages"""
        logger.info("Stopping SQS consumer...")
        self.running = False

    async def cleanup(self):
        """Cleanup resources"""
        try:
            await export_service.cleanup()
            logger.info("SQS consumer cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}", exc_info=True)


async def main():
    """Main function to run the SQS consumer"""
    consumer = SQSConsumer()
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        consumer.stop_consuming()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await consumer.initialize()
        await consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await consumer.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 