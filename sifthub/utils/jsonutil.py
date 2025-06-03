import json
from typing import Any

from starlette.concurrency import run_in_threadpool

async def convert(value: str):
    return json.loads(value)


async def jsonify(obj: Any):
    return json.dumps(obj)


async def dumps(data):
    return await run_in_threadpool(lambda: json.dumps(data))


async def loads(response):
    # Convert bytes to string if necessary
    response_str = response.decode() if isinstance(response, bytes) else str(response)
    # Run json.loads in a separate thread, with 'strict=False'
    return await run_in_threadpool(lambda: json.loads(response_str, strict=False))