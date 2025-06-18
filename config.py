import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # Database settings
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'indian_stocks')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    # Application settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 50))
    HISTORY_DAYS = int(os.getenv('HISTORY_DAYS', 365))
    UPDATE_INTERVAL_HOURS = int(os.getenv('UPDATE_INTERVAL_HOURS', 1))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Yahoo Finance settings
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', 1))
    
    @property
    def database_url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"