import os
from typing import Optional

#update after creation
#DATA_EVENT_SQS_QUEUE_URL = os.environ.get("DATA_EVENT_SQS_QUEUE_URL",
                                          #"https://sqs.us-east-2.amazonaws.com/326656825797/sqs-data-event-prod-us-east-2")
AWS_REGION = os.environ.get("REGION", 'us-east-2')

AWS_S3_BUCKET: str = os.getenv("AWS_S3_BUCKET", "sifthub-exports")
AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")