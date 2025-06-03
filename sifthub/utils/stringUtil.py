import re
from re import Match


async def empty(value: str | None) -> bool:
    return True if value is None or value == "" else False


async def equals(value: str | None, match: str) -> bool:
    return value.lower() == match.lower()


async def strip(query: str):
    if await empty(query):
        return None
    reg = re.sub(r'\s+', ' ', query)
    return reg.strip()


async def split(query: str, reg: str) -> list[str]:
    if await empty(query):
        return []
    return query.split(reg)


async def match(query: str, reg: str) -> bool:
    matched: Match[str] | None = re.match(reg, query)
    return True if matched else False


async def match_and_replace(query: str, reg: str, chars: str) -> str:
    return re.sub(reg, chars, query)


async def replace(text: str, regex: str, chars: str) -> str:
    return text.replace(regex, chars)


async def search(text: str, regex: str):
    searcher = re.search(regex, text, re.DOTALL)
    return searcher.group(1)


async def markdown_to_plain_text(text: str):
    if not text:
        return text

        # Remove bold formatting: **bold** or __bold__
    text_without_bold = re.sub(r'(\*\*|__)(.*?)\1', r'\2', text)

    text_without_links = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text_without_bold)

    return text_without_links
