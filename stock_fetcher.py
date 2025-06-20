import yfinance as yf
import pandas as pd
import logging
import time
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from config import Config
from dynamic_nse_fetcher import DynamicNSEFetcher

logger = logging.getLogger(__name__)

class IndianStockFetcher:
    def __init__(self):
        self.config = Config()
        self.dynamic_fetcher = DynamicNSEFetcher()
    
    def get_all_nse_stocks_dynamically(self, save_to_file: bool = True) -> List[Dict]:
        """Get ALL NSE stocks dynamically from real sources and optionally save to file"""
        logger.info("Fetching ALL NSE stocks dynamically...")
        stocks = self.dynamic_fetcher.get_all_nse_stocks_dynamically()
        
        return stocks
    
    def fetch_stocks_only(self) -> List[Dict]:
        """Fetch stocks without any price data"""
        logger.info("Fetching NSE stocks (no price data)...")
        return self.get_all_nse_stocks_dynamically(save_to_file=True)
    
    def get_stock_info(self, symbol: str) -> Optional[Dict]:
        """Get stock information including sector"""
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            return {
                'symbol': symbol,
                'company_name': info.get('longName', info.get('shortName', '')),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'market_cap': info.get('marketCap', 0)
            }
        except Exception as e:
            logger.error(f"Error fetching info for {symbol}: {e}")
            return None
    
    def fetch_historical_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        """Fetch historical stock data"""
        try:
            stock = yf.Ticker(symbol)
            data = stock.history(period=period)
            
            if data.empty:
                logger.warning(f"No data found for {symbol}")
                return None
            
            # Reset index to get date as column
            data.reset_index(inplace=True)
            data['Symbol'] = symbol
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def fetch_recent_data(self, symbol: str, start_date: datetime) -> Optional[pd.DataFrame]:
        """Fetch recent stock data from a specific date"""
        try:
            stock = yf.Ticker(symbol)
            end_date = datetime.now()
            
            data = stock.history(start=start_date, end=end_date)
            
            if data.empty:
                logger.info(f"No new data for {symbol} since {start_date}")
                return None
            
            data.reset_index(inplace=True)
            data['Symbol'] = symbol
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching recent data for {symbol}: {e}")
            return None
    
    def enrich_stocks_with_sectors(self, stocks: List[Dict], batch_size: int = 20) -> List[Dict]:
        """Enrich stock data with sector information from Yahoo Finance"""
        enriched_stocks = []
        
        logger.info(f"Enriching {len(stocks)} stocks with sector information...")
        
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i + batch_size]
            logger.info(f"Processing sector batch {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}")
            
            for stock in batch:
                try:
                    # Get sector info if not already present
                    if not stock.get('sector') or not stock.get('industry'):
                        symbol_without_ns = stock['symbol'].replace('.NS', '')
                        stock_info = self.get_stock_info(stock['symbol'])
                        
                        if stock_info:
                            stock['sector'] = stock_info.get('sector', stock.get('sector', ''))
                            stock['industry'] = stock_info.get('industry', stock.get('industry', ''))
                            stock['company_name'] = stock_info.get('company_name', stock.get('company_name', ''))
                            stock['market_cap'] = stock_info.get('market_cap', stock.get('market_cap', 0))
                            
                            logger.debug(f"✓ {stock['symbol']} - {stock.get('sector', 'Unknown')}")
                        else:
                            logger.debug(f"✗ {stock['symbol']} - No info available")
                    
                    enriched_stocks.append(stock)
                    
                except Exception as e:
                    logger.warning(f"Failed to enrich {stock['symbol']}: {e}")
                    enriched_stocks.append(stock)
                
                # Rate limiting
                time.sleep(0.1)
            
            # Longer delay between batches
            time.sleep(2)
        
        logger.info(f"Successfully enriched {len(enriched_stocks)} stocks with sector data")
        return enriched_stocks
        """Convert DataFrame to price records for database insertion (close price only)"""
        records = []
        
        for _, row in df.iterrows():
            record = {
                'time': row['Date'].to_pydatetime(),
                'symbol': row['Symbol'],
                'close_price': float(row['Close']) if pd.notna(row['Close']) else None
            }
            # Only add record if close price is available
            if record['close_price'] is not None:
                records.append(record)
        
        return records
    
    def fetch_all_stocks_info(self) -> List[Dict]:
        """Fetch information for ALL NSE stocks (stocks only, no prices)"""
        logger.info("Fetching ALL NSE stock information (no prices)...")
        return self.fetch_stocks_only()
    
    def fetch_all_historical_data(self, period: str = "1y") -> List[Dict]:
        """Fetch historical data for ALL NSE stocks (only if price fetching is enabled)"""
        
        if not self.config.ENABLE_PRICE_FETCHING:
            logger.warning("💰 Price fetching is DISABLED - skipping historical data fetch")
            logger.info("To enable price fetching, set ENABLE_PRICE_FETCHING=true in .env file")
            return []
        
        logger.info("💰 Price fetching is ENABLED - fetching historical data...")
        
        all_price_records = []
        
        # Get ALL NSE stocks dynamically
        nse_stocks = self.get_all_nse_stocks_dynamically(save_to_file=False)
        symbols = [stock['symbol'] for stock in nse_stocks]
        
        logger.info(f"Fetching historical data for {len(symbols)} NSE stocks...")
        
        for i, symbol in enumerate(symbols):
            logger.info(f"Fetching historical data for {symbol} ({i+1}/{len(symbols)})")
            
            # Retry logic
            for attempt in range(self.config.MAX_RETRIES):
                try:
                    df = self.fetch_historical_data(symbol, period)
                    if df is not None:
                        records = self.convert_to_price_records(df)
                        all_price_records.extend(records)
                        logger.info(f"Fetched {len(records)} records for {symbol}")
                    break
                    
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed for {symbol}: {e}")
                    if attempt < self.config.MAX_RETRIES - 1:
                        time.sleep(self.config.RETRY_DELAY)
                    else:
                        logger.error(f"Failed to fetch data for {symbol} after {self.config.MAX_RETRIES} attempts")
            
            # Add delay to avoid rate limiting
            time.sleep(0.2)
        
        return all_price_records
    
    def fetch_updates_for_symbols(self, symbols: List[str], last_update: datetime) -> List[Dict]:
        """Fetch updates for specific symbols since last update"""
        all_price_records = []
        
        for symbol in symbols:
            logger.info(f"Fetching updates for {symbol} since {last_update}")
            
            df = self.fetch_recent_data(symbol, last_update)
            if df is not None:
                records = self.convert_to_price_records(df)
                all_price_records.extend(records)
                logger.info(f"Fetched {len(records)} new records for {symbol}")
            
            time.sleep(0.1)  # Rate limiting
        
        return all_price_records
    
    def get_current_prices(self, symbols: List[str]) -> List[Dict]:
        """Get current/latest close prices for symbols (only if price fetching is enabled)"""
        
        if not self.config.ENABLE_PRICE_FETCHING:
            logger.warning("💰 Price fetching is DISABLED - skipping current price fetch")
            return []
        
        logger.info("💰 Price fetching is ENABLED - fetching current prices...")
        
        current_prices = []
        
        for symbol in symbols:
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
        
        return current_prices