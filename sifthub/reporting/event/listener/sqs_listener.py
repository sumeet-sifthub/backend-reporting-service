import asyncio

from sifthub.reporting.event.handler import export_event_handler
from sifthub.utils import jsonutil
from sifthub.utils.logger import setup_logger

logger = setup_logger()


def handler(event, context):
    if event:
        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(handle_records(event))
        return response


async def handle_records(event):
    batch_item_failures = []
    sqs_batch_response = {}
    for record in event["Records"]:
        try:
            processed = await process_message(record)
            if not processed:
                batch_item_failures.append({"itemIdentifier": record['messageId']})
        except Exception as e:
            logger.error(f"Exception occurred while processing record {record.get('messageId')}: {e}")
            batch_item_failures.append({"itemIdentifier": record['messageId']})
    sqs_batch_response["batchItemFailures"] = batch_item_failures
    logger.info(f"SQS batch response: {sqs_batch_response}")
    return sqs_batch_response


async def process_message(msg) -> bool:
    try:
        msg_body = msg.get('body')
        event_body = await jsonutil.convert(msg_body)
        event_context = None
        event_type = None
        if "Message" in event_body:
            event_context = await jsonutil.convert(event_body.get('Message'))
            event_type = event_body.get('MessageAttributes', {}).get('event_type', {}).get('Value')
        else:
            event_context = event_body
            event_type = event_context.get('event_type')
        event_response = await export_event_handler.handle_event(event_context, event_type)
        logger.info("Successfully processed export request")
        return event_response
    except Exception as e:
        logger.error(f"Exception while processing message: {e}", exc_info=True)
        return False