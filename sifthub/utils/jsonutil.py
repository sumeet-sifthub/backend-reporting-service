import json
import asyncio
from typing import Any


async def convert(value: str):
    """Convert JSON string to Python object"""
    return json.loads(value)


async def jsonify(obj: Any):
    """Convert Python object to JSON string"""
    return json.dumps(obj)


async def dumps(data):
    """Async JSON dumps"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, json.dumps, data)


async def loads(response):
    """Async JSON loads"""
    # Convert bytes to string if necessary
    response_str = response.decode() if isinstance(response, bytes) else str(response)
    # Run json.loads in a separate thread
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: json.loads(response_str, strict=False))