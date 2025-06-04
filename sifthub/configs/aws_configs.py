import os
from typing import Optional

#update after creation
DATA_REPORTING_SQS_QUEUE_URL = os.environ.get("DATA_REPORTING_SQS_QUEUE_URL",
                                          "https://sqs.us-east-1.amazonaws.com/117864947510/sqs-data-reporting-dev-us-east-1")
AWS_REGION = os.environ.get("REGION", 'us-east-1')

AWS_S3_BUCKET: str = os.getenv("AWS_S3_BUCKET", "sh-reporting-dev")
AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")