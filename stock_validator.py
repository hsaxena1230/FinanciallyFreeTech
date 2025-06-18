import yfinance as yf
import logging
from typing import List, Dict
from database import TimescaleDBManager
from config import Config

logger = logging.getLogger(__name__)

class StockValidator:
    """Utility class to validate and manage stock symbols"""
    
    def __init__(self):
        self.config = Config()
    
    def validate_stock_symbols(self, symbols: List[str]) -> Dict[str, bool]:
        """
        Validate stock symbols by checking if they exist on Yahoo Finance
        Returns dict with symbol as key and validity as value
        """
        results = {}
        
        logger.info(f"Validating {len(symbols)} stock symbols...")
        
        for i, symbol in enumerate(symbols):
            try:
                ticker = yf.Ticker(symbol)
                # Try to get 1 day of data
                hist = ticker.history(period="1d")
                
                if not hist.empty:
                    results[symbol] = True
                    logger.debug(f"✓ Valid: {symbol}")
                else:
                    results[symbol] = False
                    logger.warning(f"✗ Invalid: {symbol} - No data available")
                
                # Progress logging
                if (i + 1) % 50 == 0:
                    valid_count = sum(results.values())
                    logger.info(f"Progress: {i+1}/{len(symbols)} - {valid_count} valid symbols found")
                    
            except Exception as e:
                results[symbol] = False
                logger.warning(f"✗ Invalid: {symbol} - Error: {e}")
        
        valid_count = sum(results.values())
        logger.info(f"Validation complete: {valid_count}/{len(symbols)} symbols are valid")
        
        return results
    
    def clean_invalid_stocks(self) -> bool:
        """
        Remove invalid stock symbols from database
        """
        db = TimescaleDBManager()
        
        if not db.connect():
            logger.error("Failed to connect to database")
            return False
        
        try:
            # Get all symbols from database
            symbols = db.get_stock_symbols()
            if not symbols:
                logger.warning("No symbols found in database")
                return True
            
            logger.info(f"Validating {len(symbols)} symbols in database...")
            
            # Validate symbols
            validation_results = self.validate_stock_symbols(symbols)
            
            # Get invalid symbols
            invalid_symbols = [symbol for symbol, valid in validation_results.items() if not valid]
            
            if invalid_symbols:
                logger.info(f"Found {len(invalid_symbols)} invalid symbols, removing from database...")
                
                # Remove invalid symbols and their price data
                with db.connection.cursor() as cursor:
                    # Remove price data for invalid symbols
                    cursor.execute(
                        "DELETE FROM stock_prices WHERE symbol = ANY(%s)",
                        (invalid_symbols,)
                    )
                    
                    # Remove invalid stocks
                    cursor.execute(
                        "DELETE FROM stocks WHERE symbol = ANY(%s)",
                        (invalid_symbols,)
                    )
                    
                    db.connection.commit()
                    logger.info(f"Removed {len(invalid_symbols)} invalid symbols from database")
            else:
                logger.info("All symbols in database are valid")
            
            db.close()
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning invalid stocks: {e}")
            db.close()
            return False
    
    def get_database_stats(self) -> Dict:
        """
        Get statistics about stocks and prices in database
        """
        db = TimescaleDBManager()
        
        if not db.connect():
            logger.error("Failed to connect to database")
            return {}
        
        try:
            stats = {}
            
            with db.connection.cursor() as cursor:
                # Count stocks
                cursor.execute("SELECT COUNT(*) FROM stocks")
                stats['total_stocks'] = cursor.fetchone()[0]
                
                # Count price records
                cursor.execute("SELECT COUNT(*) FROM stock_prices")
                stats['total_price_records'] = cursor.fetchone()[0]
                
                # Get date range of price data
                cursor.execute(
                    "SELECT MIN(time), MAX(time) FROM stock_prices"
                )
                result = cursor.fetchone()
                stats['earliest_date'] = result[0]
                stats['latest_date'] = result[1]
                
                # Count stocks with recent data (last 7 days)
                cursor.execute("""
                    SELECT COUNT(DISTINCT symbol) 
                    FROM stock_prices 
                    WHERE time >= NOW() - INTERVAL '7 days'
                """)
                stats['stocks_with_recent_data'] = cursor.fetchone()[0]
                
                # Top 10 stocks by record count
                cursor.execute("""
                    SELECT symbol, COUNT(*) as record_count
                    FROM stock_prices 
                    GROUP BY symbol 
                    ORDER BY record_count DESC 
                    LIMIT 10
                """)
                stats['top_stocks_by_records'] = cursor.fetchall()
            
            db.close()
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            db.close()
            return {}