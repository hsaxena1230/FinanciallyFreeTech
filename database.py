import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from typing import List, Dict, Optional
from datetime import datetime
from config import Config

logger = logging.getLogger(__name__)

class TimescaleDBManager:
    def __init__(self):
        self.config = Config()
        self.connection = None
        
    def connect(self):
        """Connect to TimescaleDB"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.DB_HOST,
                port=self.config.DB_PORT,
                database=self.config.DB_NAME,
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD
            )
            logger.info("Connected to TimescaleDB successfully")
            return True
        except psycopg2.Error as e:
            logger.error(f"Error connecting to database: {e}")
            return False
    
    def create_database_if_not_exists(self):
        """Create database if it doesn't exist"""
        try:
            # Connect to postgres database to create our target database
            conn = psycopg2.connect(
                host=self.config.DB_HOST,
                port=self.config.DB_PORT,
                database='postgres',
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            with conn.cursor() as cursor:
                # Check if database exists
                cursor.execute(
                    "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                    (self.config.DB_NAME,)
                )
                exists = cursor.fetchone()
                
                if not exists:
                    cursor.execute(f'CREATE DATABASE "{self.config.DB_NAME}"')
                    logger.info(f"Database '{self.config.DB_NAME}' created successfully")
                else:
                    logger.info(f"Database '{self.config.DB_NAME}' already exists")
            
            conn.close()
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Error creating database: {e}")
            return False
    
    def initialize_database(self):
        """Initialize database with TimescaleDB extension and create tables"""
        if not self.connect():
            return False
            
        try:
            with self.connection.cursor() as cursor:
                # Create TimescaleDB extension
                cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
                logger.info("TimescaleDB extension created/verified")
                
                # Create stocks table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS stocks (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(20) UNIQUE NOT NULL,
                        company_name VARCHAR(255),
                        sector VARCHAR(100),
                        industry VARCHAR(100),
                        market_cap BIGINT,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    );
                """)
                
                # Create stock_prices table (only close price)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS stock_prices (
                        time TIMESTAMPTZ NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        close_price DECIMAL(12,4) NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (time, symbol)
                    );
                """)
                
                # Create hypertable
                cursor.execute("""
                    SELECT create_hypertable('stock_prices', 'time', 
                                            chunk_time_interval => INTERVAL '1 day',
                                            if_not_exists => TRUE);
                """)
                
                # Create indexes for better performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_stocks_sector 
                    ON stocks (sector);
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_stocks_industry 
                    ON stocks (industry);
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_time 
                    ON stock_prices (symbol, time DESC);
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_stock_prices_time 
                    ON stock_prices (time DESC);
                """)
                
                self.connection.commit()
                logger.info("Database schema initialized successfully")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Error initializing database: {e}")
            self.connection.rollback()
            return False
    
    def insert_stocks(self, stocks: List[Dict]):
        """Insert stock metadata"""
        if not self.connection:
            return False
            
        try:
            with self.connection.cursor() as cursor:
                for stock in stocks:
                    cursor.execute("""
                        INSERT INTO stocks (symbol, company_name, sector, industry, market_cap)
                        VALUES (%(symbol)s, %(company_name)s, %(sector)s, %(industry)s, %(market_cap)s)
                        ON CONFLICT (symbol) 
                        DO UPDATE SET 
                            company_name = EXCLUDED.company_name,
                            sector = EXCLUDED.sector,
                            industry = EXCLUDED.industry,
                            market_cap = EXCLUDED.market_cap,
                            updated_at = NOW()
                    """, stock)
                
                self.connection.commit()
                logger.info(f"Inserted/updated {len(stocks)} stocks")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Error inserting stocks: {e}")
            self.connection.rollback()
            return False
    
    def insert_stock_prices(self, prices: List[Dict]):
        """Insert stock price data"""
        if not self.connection:
            return False
            
        try:
            with self.connection.cursor() as cursor:
                # Use execute_values for better performance
                psycopg2.extras.execute_values(
                    cursor,
                    """
                    INSERT INTO stock_prices 
                    (time, symbol, close_price)
                    VALUES %s
                    ON CONFLICT (time, symbol) 
                    DO UPDATE SET 
                        close_price = EXCLUDED.close_price
                    """,
                    [
                        (
                            price['time'],
                            price['symbol'],
                            price['close_price']
                        )
                        for price in prices
                    ]
                )
                
                self.connection.commit()
                logger.info(f"Inserted/updated {len(prices)} price records")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Error inserting stock prices: {e}")
            self.connection.rollback()
            return False
    
    def get_latest_price_date(self, symbol: str) -> Optional[datetime]:
        """Get the latest price date for a symbol"""
        if not self.connection:
            return None
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT MAX(time) FROM stock_prices WHERE symbol = %s",
                    (symbol,)
                )
                result = cursor.fetchone()
                return result[0] if result[0] else None
                
        except psycopg2.Error as e:
            logger.error(f"Error getting latest price date: {e}")
            return None
    
    def get_stock_symbols(self) -> List[str]:
        """Get all stock symbols from database"""
        if not self.connection:
            return []
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT symbol FROM stocks ORDER BY symbol")
                return [row[0] for row in cursor.fetchall()]
                
        except psycopg2.Error as e:
            logger.error(f"Error getting stock symbols: {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")