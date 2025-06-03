async def get_object_name(key):
    try:
        object_name = key.rsplit('/', 1)  # Extract the name from the key
        return object_name[0], object_name[-1]
    except Exception as e:
        print(f"Error: {e}")
        return None, None


async def extension_from_url(s3_url: str):
    _, file_name = await get_object_name(s3_url)
    return file_name.split(".")[-1]
