import os

# API Configuration
BATCH_SIZE = 100

# Export Configuration
EXPORT_FILE_EXPIRY_HOURS: int = int(os.getenv("EXPORT_FILE_EXPIRY_HOURS", "24"))
MAX_EXPORT_SIZE_MB: int = int(os.getenv("MAX_EXPORT_SIZE_MB", "100")) 