import httpx

from typing import Any, Optional, Dict

from sifthub.configs.http_configs import HTTP_PROTOCOL
from sifthub.utils.logger import setup_logger

logger = setup_logger()

http = HTTP_PROTOCOL


async def get(service_name: str, end_point: str, params, headers):
    async with httpx.AsyncClient(verify=False) as client:
        url = http + service_name + end_point
        response = await client.get(
            url,
            headers=headers,
            params=params
        )
    return None if response.text == '' else response.json()


async def put(service_name: str, end_point: str, request_data, params, headers):
    async with httpx.AsyncClient(verify=False) as client:
        url = http + service_name + end_point
        logger.info(f"Put url is [{url}]")
        response = await client.put(
            url,
            json=request_data,
            headers=headers,
            params=params
        )
    return None if response.text == '' else response.json()


async def post_response(service_name: str, end_point: str, json: Optional[Any],
                        params: Optional[Dict] = None,
                        headers: Optional[Dict] = None):
    url = http + service_name + end_point
    async with httpx.AsyncClient(verify=False, timeout=httpx.Timeout(10.0, read=180.0)) as client:
        response = await client.post(
            url,
            json=json,
            headers=headers,
            params=params,
            timeout=None
        )
        return response


async def post(service_name: str, end_point: str, json: Optional[Any],
               params: Optional[Dict] = None,
               headers: Optional[Dict] = None):
    response = await post_response(service_name, end_point, json, params, headers)
    return None if response.text == '' else response.json()