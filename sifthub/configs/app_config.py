import os

APP_HOST = "0.0.0.0"
APP_PORT = 8087
    
# Firebase Configuration
FIREBASE_SECRETS_PATH: str = os.getenv("FIREBASE_SECRETS_PATH", "notifications/internal/FIREBASE")
    
# Logging
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")