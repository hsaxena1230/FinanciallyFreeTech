def fetch_stocks_to_file():
    """Fetch NSE stocks and save to file (no database, no prices)"""
    logger = logging.getLogger(__name__)
    config = Config()
    fetcher = IndianStockFetcher()
    
    logger.info("🔄 Fetching NSE stocks and saving to files...")
    logger.info(f"📁 Output file: {config.STOCKS_OUTPUT_FILE}")
    
    # Fetch stocks
    stocks = fetcher.fetch_stocks_only()
    
    if stocks:
        logger.info(f"✅ Successfully fetched {len(stocks)} NSE stocks")
        return True
    else:
        logger.error("❌ Failed to fetch stocks")
        return False

def enrich_existing_stocks_with_sectors():
    """Enrich existing stocks in database with sector information"""
    logger = logging.getLogger(__name__)
    db = TimescaleDBManager()
    fetcher = IndianStockFetcher()
    
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    # Get all symbols from database that need sector data
    try:
        with db.connection.cursor() as cursor:
            cursor.execute("""
                SELECT symbol, company_name, sector, industry 
                FROM stocks 
                WHERE sector IS NULL OR sector = '' OR industry IS NULL OR industry = ''
                ORDER BY symbol
            """)
            stocks_to_enrich = cursor.fetchall()
            
        if not stocks_to_enrich:
            logger.info("✅ All stocks already have sector information")
            db.close()
            return True
            
        logger.info(f"Found {len(stocks_to_enrich)} stocks needing sector information")
        
        # Process in batches
        batch_size = 20
        enriched_count = 0
        
        for i in range(0, len(stocks_to_enrich), batch_size):
            batch = stocks_to_enrich[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(stocks_to_enrich)-1)//batch_size + 1}")
            
            for symbol, company_name, current_sector, current_industry in batch:
                try:
                    # Get sector info from Yahoo Finance
                    stock_info = fetcher.get_stock_info(symbol)
                    
                    if stock_info:
                        new_sector = stock_info.get('sector', '').strip()
                        new_industry = stock_info.get('industry', '').strip()
                        new_company_name = stock_info.get('company_name', '').strip()
                        market_cap = stock_info.get('market_cap', 0)
                        
                        # Update database if we got new information
                        if new_sector or new_industry or new_company_name:
                            with db.connection.cursor() as cursor:
                                cursor.execute("""
                                    UPDATE stocks 
                                    SET sector = COALESCE(NULLIF(%s, ''), sector),
                                        industry = COALESCE(NULLIF(%s, ''), industry),
                                        company_name = COALESCE(NULLIF(%s, ''), company_name),
                                        market_cap = COALESCE(NULLIF(%s, 0), market_cap),
                                        updated_at = NOW()
                                    WHERE symbol = %s
                                """, (new_sector, new_industry, new_company_name, market_cap, symbol))
                            
                            db.connection.commit()
                            enriched_count += 1
                            
                            logger.info(f"✓ {symbol}: {new_sector} - {new_industry}")
                        else:
                            logger.debug(f"✗ {symbol}: No sector info available")
                    else:
                        logger.debug(f"✗ {symbol}: Failed to get stock info")
                        
                except Exception as e:
                    logger.warning(f"Error enriching {symbol}: {e}")
                    continue
                
                # Rate limiting
                time.sleep(0.2)
            
            # Longer delay between batches
            logger.info(f"Completed batch {i//batch_size + 1}, waiting 3 seconds...")
            time.sleep(3)
        
        logger.info(f"✅ Successfully enriched {enriched_count} stocks with sector information")
        db.close()
        return True
        
    except Exception as e:
        logger.error(f"Error during sector enrichment: {e}")
        db.close()
        return False
    """Apply database migrations to update schema"""
    logger = logging.getLogger(__name__)
    db = TimescaleDBManager()
    
    logger.info("Applying database migrations...")
    if db.migrate_database():
        logger.info("Database migration completed successfully")
        db.close()
        return True
    else:
        logger.error("Database migration failed")
        db.close()
        return False

def setup_stocks():
    """Setup stock metadata in database (ALL NSE stocks dynamically)"""
    logger = logging.getLogger(__name__)
    config = Config()
    db = TimescaleDBManager()
    fetcher = IndianStockFetcher()
    
    if not config.ENABLE_PRICE_FETCHING:
        logger.warning("💰 Price fetching is DISABLED")
        logger.info("📝 Use --fetch-stocks to save stocks to file without database")
        logger.info("💡 To enable price fetching, set ENABLE_PRICE_FETCHING=true in .env file")
        return False
    
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    logger.info("Fetching ALL NSE stock information dynamically...")
    stocks_info = fetcher.fetch_all_stocks_info()
    
    if stocks_info:
        logger.info(f"Inserting {len(stocks_info)} stocks into database...")
        
        # Insert in batches to handle large number of stocks
        batch_size = 50
        success_count = 0
        
        for i in range(0, len(stocks_info), batch_size):
            batch = stocks_info[i:i + batch_size]
            if db.insert_stocks(batch):
                success_count += len(batch)
"""
Indian Stock Price Tracker
Fetches Indian stock data from Yahoo Finance and stores in TimescaleDB
"""

import argparse
import logging
import schedule
import time
from datetime import datetime, timedelta
from database import TimescaleDBManager
from stock_fetcher import IndianStockFetcher
from stock_validator import StockValidator
from config import Config

def setup_logging():
    """Setup logging configuration"""
    config = Config()
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('stock_tracker.log'),
            logging.StreamHandler()
        ]
    )

def initialize_database():
    """Initialize the database and create tables"""
    logger = logging.getLogger(__name__)
    db = TimescaleDBManager()
    
    # Create database if not exists
    logger.info("Creating database if not exists...")
    if not db.create_database_if_not_exists():
        logger.error("Failed to create database")
        return False
    
    # Initialize database schema
    logger.info("Initializing database schema...")
    if not db.initialize_database():
        logger.error("Failed to initialize database schema")
        return False
    
    db.close()
    logger.info("Database initialization completed successfully")
    return True

def setup_stocks():
    """Setup stock metadata in database (ALL NSE stocks dynamically)"""
    logger = logging.getLogger(__name__)
    db = TimescaleDBManager()
    fetcher = IndianStockFetcher()
    
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    logger.info("Fetching ALL NSE stock information dynamically...")
    stocks_info = fetcher.fetch_all_stocks_info()
    
    if stocks_info:
        logger.info(f"Inserting {len(stocks_info)} stocks into database...")
        
        # Insert in batches to handle large number of stocks
        batch_size = 50
        success_count = 0
        
        for i in range(0, len(stocks_info), batch_size):
            batch = stocks_info[i:i + batch_size]
            if db.insert_stocks(batch):
                success_count += len(batch)
                logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} stocks")
            else:
                logger.error(f"Failed to insert batch {i//batch_size + 1}")
        
        if success_count > 0:
            logger.info(f"Stock metadata setup completed successfully: {success_count} stocks inserted")
            db.close()
            return True
    
    logger.error("Failed to setup stocks")
    db.close()
    return False

def test_dynamic_fetch():
    """Test the dynamic NSE fetching capability"""
    logger = logging.getLogger(__name__)
    fetcher = IndianStockFetcher()
    
    logger.info("Testing dynamic NSE stock fetching...")
    
    # Fetch stocks dynamically (no prices)
    stocks = fetcher.fetch_stocks_only()
    
    if stocks:
        logger.info(f"✅ Successfully fetched {len(stocks)} stocks dynamically")
        
        # Show some sample stocks
        logger.info("Sample stocks:")
        for i, stock in enumerate(stocks[:10]):
            logger.info(f"  {i+1}. {stock['symbol']} - {stock.get('company_name', 'N/A')}")
        
        return True
    else:
        logger.error("❌ Failed to fetch any stocks dynamically")
        return False

def load_historical_data():
    """Load historical stock price data"""
    logger = logging.getLogger(__name__)
    config = Config()
    db = TimescaleDBManager()
    fetcher = IndianStockFetcher()
    
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    logger.info("Fetching historical stock data...")
    
    # Calculate period based on HISTORY_DAYS
    if config.HISTORY_DAYS <= 7:
        period = "7d"
    elif config.HISTORY_DAYS <= 30:
        period = "1mo"
    elif config.HISTORY_DAYS <= 90:
        period = "3mo"
    elif config.HISTORY_DAYS <= 180:
        period = "6mo"
    else:
        period = "1y"
    
    price_records = fetcher.fetch_all_historical_data(period)
    
    if price_records:
        logger.info(f"Inserting {len(price_records)} price records into database...")
        
        # Insert in batches
        batch_size = config.BATCH_SIZE
        for i in range(0, len(price_records), batch_size):
            batch = price_records[i:i + batch_size]
            if db.insert_stock_prices(batch):
                logger.info(f"Inserted batch {i//batch_size + 1}")
            else:
                logger.error(f"Failed to insert batch {i//batch_size + 1}")
        
        logger.info("Historical data loading completed")
        db.close()
        return True
    
    logger.error("Failed to load historical data")
    db.close()
    return False

def setup_quick_stocks(stocks_list):
    """Setup quick stock list in database"""
    logger = logging.getLogger(__name__)
    db = TimescaleDBManager()
    
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    logger.info(f"Inserting {len(stocks_list)} popular stocks into database...")
    if db.insert_stocks(stocks_list):
        logger.info("Popular stocks setup completed successfully")
        db.close()
        return True
    
    logger.error("Failed to setup popular stocks")
    db.close()
    return False

def load_historical_data():
    """Load historical stock price data for stocks in database"""
    logger = logging.getLogger(__name__)
    config = Config()
    db = TimescaleDBManager()
    fetcher = IndianStockFetcher()
    
    logger.info("Loading historical price data...")
    
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    # Check how many stocks we have in database
    symbols = db.get_stock_symbols()
    
    if not symbols:
        logger.error("No stocks found in database. Run --setup-stocks first.")
        db.close()
        return False
    
    logger.info(f"Found {len(symbols)} stocks in database, loading historical data...")
    
    # Calculate period based on HISTORY_DAYS
    if config.HISTORY_DAYS <= 7:
        period = "7d"
    elif config.HISTORY_DAYS <= 30:
        period = "1mo"
    elif config.HISTORY_DAYS <= 90:
        period = "3mo"
    elif config.HISTORY_DAYS <= 180:
        period = "6mo"
    else:
        period = "1y"
    
    # Fetch historical data for symbols in database
    all_price_records = []
    
    for i, symbol in enumerate(symbols):
        logger.info(f"Fetching historical data for {symbol} ({i+1}/{len(symbols)})")
        
        # Retry logic
        for attempt in range(config.MAX_RETRIES):
            try:
                df = fetcher.fetch_historical_data(symbol, period)
                if df is not None:
                    records = fetcher.convert_to_price_records(df)
                    all_price_records.extend(records)
                    logger.info(f"Fetched {len(records)} records for {symbol}")
                break
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {symbol}: {e}")
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(config.RETRY_DELAY)
                else:
                    logger.error(f"Failed to fetch data for {symbol} after {config.MAX_RETRIES} attempts")
        
        # Add delay to avoid rate limiting
        time.sleep(0.2)
    
    if all_price_records:
        logger.info(f"Inserting {len(all_price_records)} price records into database...")
        
        # Insert in batches
        batch_size = config.BATCH_SIZE
        for i in range(0, len(all_price_records), batch_size):
            batch = all_price_records[i:i + batch_size]
            if db.insert_stock_prices(batch):
                logger.info(f"Inserted batch {i//batch_size + 1}")
            else:
                logger.error(f"Failed to insert batch {i//batch_size + 1}")
        
        logger.info("Historical data loading completed")
        db.close()
        return True
    
    logger.error("Failed to load historical data")
    db.close()
    return False

def update_stock_prices():
    """Update stock prices with latest data"""
    logger = logging.getLogger(__name__)
    db = TimescaleDBManager()
    fetcher = IndianStockFetcher()
    
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    # Get all symbols from database
    symbols = db.get_stock_symbols()
    if not symbols:
        logger.warning("No stocks found in database. Run --setup-stocks first.")
        db.close()
        return False
    
    logger.info(f"Updating prices for {len(symbols)} stocks...")
    
    # Get current prices for all symbols
    current_prices = []
    
    for i, symbol in enumerate(symbols):
        logger.info(f"Fetching current price for {symbol} ({i+1}/{len(symbols)})")
        try:
            stock = yf.Ticker(symbol)
            # Get today's data
            data = stock.history(period="1d")
            
            if not data.empty:
                latest = data.iloc[-1]
                record = {
                    'time': datetime.now().replace(microsecond=0),
                    'symbol': symbol,
                    'close_price': float(latest['Close']) if pd.notna(latest['Close']) else None
                }
                # Only add if close price is available
                if record['close_price'] is not None:
                    current_prices.append(record)
                    
        except Exception as e:
            logger.error(f"Error fetching current price for {symbol}: {e}")
        
        time.sleep(0.1)  # Rate limiting
    
    if current_prices:
        logger.info(f"Inserting {len(current_prices)} current price records...")
        if db.insert_stock_prices(current_prices):
            logger.info("Stock prices updated successfully")
            db.close()
            return True
    
    logger.error("Failed to update stock prices")
    db.close()
    return False

def update_stock_prices():
    """Update stock prices with latest data"""
    logger = logging.getLogger(__name__)
    db = TimescaleDBManager()
    fetcher = IndianStockFetcher()
    
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    # Get all symbols from database
    symbols = db.get_stock_symbols()
    if not symbols:
        logger.warning("No stocks found in database")
        db.close()
        return False
    
    logger.info(f"Updating prices for {len(symbols)} stocks...")
    
    # Get current prices
    current_prices = fetcher.get_current_prices(symbols)
    
    if current_prices:
        logger.info(f"Inserting {len(current_prices)} current price records...")
        if db.insert_stock_prices(current_prices):
            logger.info("Stock prices updated successfully")
            db.close()
            return True
    
    logger.error("Failed to update stock prices")
    db.close()
    return False

def show_database_stats():
    """Show database statistics"""
    logger = logging.getLogger(__name__)
    validator = StockValidator()
    
    logger.info("Fetching database statistics...")
    stats = validator.get_database_stats()
    
    if stats:
        logger.info("=== DATABASE STATISTICS ===")
        logger.info(f"Total stocks: {stats.get('total_stocks', 0)}")
        logger.info(f"Total price records: {stats.get('total_price_records', 0)}")
        logger.info(f"Stocks with recent data (7 days): {stats.get('stocks_with_recent_data', 0)}")
        
        if stats.get('earliest_date') and stats.get('latest_date'):
            logger.info(f"Date range: {stats['earliest_date']} to {stats['latest_date']}")
        
        if stats.get('top_stocks_by_records'):
            logger.info("Top 10 stocks by record count:")
            for symbol, count in stats['top_stocks_by_records']:
                logger.info(f"  {symbol}: {count} records")
    else:
        logger.error("Failed to fetch database statistics")

def validate_and_clean():
    """Validate stock symbols and clean invalid ones"""
    logger = logging.getLogger(__name__)
    validator = StockValidator()
    
    logger.info("Starting stock validation and cleanup...")
    if validator.clean_invalid_stocks():
        logger.info("Stock validation and cleanup completed successfully")
        return True
    else:
        logger.error("Stock validation and cleanup failed")
        return False
    """Run the scheduler for periodic updates"""
    logger = logging.getLogger(__name__)
    config = Config()
    
    # Schedule price updates
    schedule.every(config.UPDATE_INTERVAL_HOURS).hours.do(update_stock_prices)
    
    logger.info(f"Scheduler started. Will update prices every {config.UPDATE_INTERVAL_HOURS} hours")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")

def main():
    """Main function"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    parser = argparse.ArgumentParser(description='Indian Stock Price Tracker')
    parser.add_argument('--init', action='store_true', help='Initialize database')
    parser.add_argument('--migrate', action='store_true', help='Apply database migrations')
    parser.add_argument('--setup-stocks', action='store_true', help='Setup ALL NSE stocks in database (no prices)')
    parser.add_argument('--enrich-sectors', action='store_true', help='Enrich existing stocks with sector information')
    parser.add_argument('--load-history', action='store_true', help='Load historical price data for stocks in database')
    parser.add_argument('--update', action='store_true', help='Update current prices for stocks in database')
    parser.add_argument('--schedule', action='store_true', help='Run scheduler for periodic updates')
    parser.add_argument('--full-setup', action='store_true', help='Run complete setup (init + stocks + history)')
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    parser.add_argument('--validate', action='store_true', help='Validate and clean invalid stock symbols')
    parser.add_argument('--test-fetch', action='store_true', help='Test dynamic NSE stock fetching')
    parser.add_argument('--fetch-stocks', action='store_true', help='Fetch stocks and save to file (no database)')
    parser.add_argument('--quick-setup', action='store_true', help='Quick setup with popular stocks only')
    
    args = parser.parse_args()
    
    if args.init:
        logger.info("Initializing database...")
        if initialize_database():
            logger.info("Database initialization completed")
        else:
            logger.error("Database initialization failed")
            return 1
    
    elif args.migrate:
        logger.info("Applying database migrations...")
        if migrate_database():
            logger.info("Database migration completed")
        else:
            logger.error("Database migration failed")
            return 1
    
    elif args.enrich_sectors:
        logger.info("Enriching existing stocks with sector information...")
        if enrich_existing_stocks_with_sectors():
            logger.info("Sector enrichment completed successfully")
        else:
            logger.error("Sector enrichment failed")
            return 1
    
    elif args.setup_stocks:
        logger.info("Setting up stocks...")
        if setup_stocks():
            logger.info("Stock setup completed")
        else:
            logger.error("Stock setup failed")
            return 1
    
    elif args.load_history:
        logger.info("Loading historical data...")
        if load_historical_data():
            logger.info("Historical data loading completed")
        else:
            logger.error("Historical data loading failed")
            return 1
    
    elif args.update:
        logger.info("Updating stock prices...")
        if update_stock_prices():
            logger.info("Stock prices updated")
        else:
            logger.error("Stock price update failed")
            return 1
    
    elif args.stats:
        logger.info("Showing database statistics...")
        show_database_stats()
    
    elif args.validate:
        logger.info("Validating and cleaning stock symbols...")
        if validate_and_clean():
            logger.info("Validation and cleanup completed")
        else:
            logger.error("Validation and cleanup failed")
            return 1
    
    elif args.fetch_stocks:
        logger.info("Fetching stocks and saving to file...")
        if fetch_stocks_to_file():
            logger.info("Stock fetching completed successfully")
        else:
            logger.error("Stock fetching failed")
            return 1
    
    elif args.test_fetch:
        logger.info("Testing dynamic NSE stock fetching...")
        if test_dynamic_fetch():
            logger.info("Dynamic fetch test completed successfully")
        else:
            logger.error("Dynamic fetch test failed")
            return 1
    
    elif args.quick_setup:
        logger.info("Running quick setup with popular stocks...")
        
        # Use the old popular stocks approach for quick setup
        from nse_symbol_fetcher import NSESymbolFetcher
        nse_fetcher = NSESymbolFetcher()
        
        # Create quick list from popular stocks
        quick_stocks = nse_fetcher._create_popular_stocks_list()
        
        steps = [
            ("Initializing database", initialize_database),
            ("Setting up popular stocks", lambda: setup_quick_stocks(quick_stocks)),
            ("Loading historical data", load_historical_data)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"Step: {step_name}")
            if not step_func():
                logger.error(f"Failed at step: {step_name}")
                return 1
            logger.info(f"Completed: {step_name}")
        
        logger.info("Quick setup completed successfully!")
        logger.info("You can now run: python main.py --schedule")
    
    elif args.schedule:
        logger.info("Starting scheduler...")
        run_scheduler()
    
    elif args.full_setup:
        logger.info("Running full setup...")
        
        steps = [
            ("Initializing database", initialize_database),
            ("Applying migrations", migrate_database),
            ("Setting up stocks", setup_stocks),
            ("Enriching with sectors", enrich_existing_stocks_with_sectors),
            ("Loading historical data", load_historical_data)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"Step: {step_name}")
            if not step_func():
                logger.error(f"Failed at step: {step_name}")
                return 1
            logger.info(f"Completed: {step_name}")
        
        logger.info("Full setup completed successfully!")
        logger.info("Note: This setup stocks first, then fetches historical prices.")
        logger.info("You can now run: python main.py --schedule")
    
    else:
        parser.print_help()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())