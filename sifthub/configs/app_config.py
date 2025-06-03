import os

APP_HOST = "0.0.0.0"
APP_PORT = 8087
    
# Firebase Configuration
FIREBASE_SECRETS_PATH: str = os.getenv("FIREBASE_SECRETS_PATH", "notifications/internal/FIREBASE")
    
# Export Configuration
EXPORT_FILE_EXPIRY_HOURS: int = int(os.getenv("EXPORT_FILE_EXPIRY_HOURS", "24"))
MAX_EXPORT_SIZE_MB: int = int(os.getenv("MAX_EXPORT_SIZE_MB", "100"))
    
# Logging
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")