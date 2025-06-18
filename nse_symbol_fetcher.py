import requests
import pandas as pd
import yfinance as yf
import logging
import time
from typing import List, Dict, Optional
from datetime import datetime
import json
import re

logger = logging.getLogger(__name__)

class NSESymbolFetcher:
    def __init__(self):
        self.base_nse_url = "https://www.nseindia.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Backup hardcoded popular NSE stocks
        self.popular_nse_stocks = [
            # Banking & Financial Services
            'HDFCBANK', 'ICICIBANK', 'SBIN', 'AXISBANK', 'KOTAKBANK', 'INDUSINDBK',
            'BAJFINANCE', 'BAJAJFINSV', 'HDFCLIFE', 'SBILIFE', 'ICICIPRULI',
            'BAJAJFINSERV', 'PNB', 'BANKBARODA', 'FEDERALBNK', 'IDFCFIRSTB',
            
            # Information Technology
            'TCS', 'INFY', 'HCLTECH', 'WIPRO', 'TECHM', 'MINDTREE', 'MPHASIS',
            'LTTS', 'PERSISTENT', 'COFORGE', 'LTIM', 'OFSS',
            
            # Oil & Gas
            'RELIANCE', 'ONGC', 'BPCL', 'IOC', 'GAIL', 'HINDPETRO',
            
            # Consumer Goods
            'HINDUNILVR', 'ITC', 'NESTLEIND', 'BRITANNIA', 'DABUR', 'GODREJCP',
            'MARICO', 'COLPAL', 'PGHH', 'VBL', 'TATACONSUM',
            
            # Automobile
            'MARUTI', 'TATAMOTORS', 'BAJAJ-AUTO', 'HEROMOTOCO', 'EICHERMOT',
            'M&M', 'ASHOKLEY', 'BHARATFORG', 'MOTHERSON', 'BOSCHLTD',
            
            # Pharmaceuticals
            'SUNPHARMA', 'DRREDDY', 'CIPLA', 'DIVISLAB', 'BIOCON', 'LUPIN',
            'CADILAHC', 'TORNTPHARM', 'GLENMARK', 'AUROPHARMA', 'ALKEM',
            
            # Metals & Mining
            'TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'VEDL', 'SAIL', 'NMDC',
            'COALINDIA', 'JINDALSTEL', 'RATNAMANI', 'WELCORP',
            
            # Cement
            'ULTRACEMCO', 'SHREECEM', 'GRASIM', 'ACC', 'AMBUJACEMENT',
            'HEIDELBERG', 'RAMCOCEM', 'JKCEMENT',
            
            # Infrastructure & Construction
            'LT', 'POWERGRID', 'NTPC', 'ADANIPORTS', 'GMRINFRA', 'IRB',
            
            # Textiles
            'GUJGAS', 'WELSPUNIND', 'TRIDENT', 'VARDHMANTEXT',
            
            # Retail
            'DMART', 'TRENT', 'JUBLFOOD', 'WESTLIFE',
            
            # Airlines & Transportation
            'INDIGO', 'SPICEJET',
            
            # Telecom
            'BHARTIARTL', 'IDEA',
            
            # Power
            'ADANIGREEN', 'ADANITRANS', 'TATAPOWER', 'TORNTPOWER',
            
            # Others
            'ASIANPAINT', 'TITAN', 'PIDILITIND', 'BERGEPAINT', 'KANSAINER',
            'DIXON', 'WHIRLPOOL', 'HAVELLS', 'VOLTAS', 'BLUESTARCO'
        ]
    
    def fetch_nse_equity_list(self) -> List[Dict]:
        """
        Fetch NSE equity list using multiple methods
        """
        symbols = []
        
        # Method 1: Try to fetch from NSE website
        try:
            symbols = self._fetch_from_nse_api()
            if symbols:
                logger.info(f"Fetched {len(symbols)} symbols from NSE API")
                return symbols
        except Exception as e:
            logger.warning(f"Failed to fetch from NSE API: {e}")
        
        # Method 2: Try alternative NSE endpoints
        try:
            symbols = self._fetch_from_alternative_endpoints()
            if symbols:
                logger.info(f"Fetched {len(symbols)} symbols from alternative endpoints")
                return symbols
        except Exception as e:
            logger.warning(f"Failed to fetch from alternative endpoints: {e}")
        
        # Method 3: Use popular stocks as fallback
        logger.info("Using popular NSE stocks as fallback")
        return self._create_popular_stocks_list()
    
    def _fetch_from_nse_api(self) -> List[Dict]:
        """
        Try to fetch symbols from NSE API endpoints
        """
        try:
            # Get session cookies first
            self.session.get(self.base_nse_url)
            
            # Try equity master API
            equity_url = f"{self.base_nse_url}/api/equity-master"
            response = self.session.get(equity_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                symbols = []
                
                for stock in data:
                    if isinstance(stock, dict) and 'symbol' in stock:
                        symbols.append({
                            'symbol': stock['symbol'] + '.NS',
                            'company_name': stock.get('companyName', ''),
                            'sector': stock.get('industry', ''),
                            'market_cap': None
                        })
                
                return symbols
        except Exception as e:
            logger.error(f"Error fetching from NSE API: {e}")
        
        return []
    
    def _fetch_from_alternative_endpoints(self) -> List[Dict]:
        """
        Try alternative methods to fetch NSE symbols
        """
        symbols = []
        
        try:
            # Method: Screen scraping approach
            search_url = f"{self.base_nse_url}/api/search/autocomplete"
            params = {'q': 'A'}  # Start with 'A' to get some symbols
            
            response = self.session.get(search_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'symbols' in data:
                    for symbol_data in data['symbols'][:100]:  # Limit to prevent overload
                        if symbol_data.get('symbol'):
                            symbols.append({
                                'symbol': symbol_data['symbol'] + '.NS',
                                'company_name': symbol_data.get('symbol_info', ''),
                                'sector': '',
                                'market_cap': None
                            })
        except Exception as e:
            logger.error(f"Error in alternative fetch: {e}")
        
        return symbols
    
    def _create_popular_stocks_list(self) -> List[Dict]:
        """
        Create list from popular NSE stocks as fallback
        """
        symbols = []
        
        for symbol in self.popular_nse_stocks:
            symbols.append({
                'symbol': symbol + '.NS',
                'company_name': '',  # Will be fetched later from Yahoo Finance
                'sector': '',
                'market_cap': None
            })
        
        logger.info(f"Created list of {len(symbols)} popular NSE stocks")
        return symbols
    
    def enrich_stock_data(self, symbols: List[Dict], batch_size: int = 10) -> List[Dict]:
        """
        Enrich stock data with company information from Yahoo Finance
        """
        enriched_symbols = []
        
        logger.info(f"Enriching data for {len(symbols)} symbols...")
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(symbols)-1)//batch_size + 1}")
            
            for stock in batch:
                try:
                    # Fetch stock info from Yahoo Finance
                    ticker = yf.Ticker(stock['symbol'])
                    info = ticker.info
                    
                    # Update stock data with Yahoo Finance info
                    stock['company_name'] = info.get('longName', info.get('shortName', ''))
                    stock['sector'] = info.get('sector', '')
                    stock['market_cap'] = info.get('marketCap', 0)
                    
                    enriched_symbols.append(stock)
                    logger.debug(f"Enriched data for {stock['symbol']}")
                    
                except Exception as e:
                    # Keep original data if enrichment fails
                    logger.warning(f"Failed to enrich {stock['symbol']}: {e}")
                    enriched_symbols.append(stock)
                
                # Rate limiting
                time.sleep(0.1)
            
            # Longer delay between batches
            time.sleep(1)
        
        logger.info(f"Successfully enriched {len(enriched_symbols)} symbols")
        return enriched_symbols
    
    def validate_symbols(self, symbols: List[Dict]) -> List[Dict]:
        """
        Validate symbols by checking if they exist on Yahoo Finance
        """
        valid_symbols = []
        
        logger.info(f"Validating {len(symbols)} symbols...")
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol['symbol'])
                # Try to get basic info - if it fails, symbol is invalid
                hist = ticker.history(period="1d")
                
                if not hist.empty:
                    valid_symbols.append(symbol)
                    logger.debug(f"✓ Valid: {symbol['symbol']}")
                else:
                    logger.warning(f"✗ Invalid: {symbol['symbol']} - No data")
                    
            except Exception as e:
                logger.warning(f"✗ Invalid: {symbol['symbol']} - {e}")
            
            # Rate limiting
            time.sleep(0.1)
        
        logger.info(f"Validated {len(valid_symbols)} out of {len(symbols)} symbols")
        return valid_symbols
    
    def get_nse_symbols_from_csv_sources(self) -> List[Dict]:
        """
        Fetch NSE symbols from known CSV sources
        """
        symbols = []
        
        # Known NSE symbol lists (these are example URLs - you'd need working ones)
        csv_sources = [
            # These are example URLs - in practice, you'd need working download links
            # "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
            # "https://www1.nseindia.com/content/equities/EQUITY_L.csv"
        ]
        
        for source_url in csv_sources:
            try:
                logger.info(f"Trying to fetch from: {source_url}")
                response = self.session.get(source_url, timeout=30)
                
                if response.status_code == 200:
                    # Parse CSV content
                    from io import StringIO
                    csv_data = pd.read_csv(StringIO(response.text))
                    
                    for _, row in csv_data.iterrows():
                        symbol = row.get('SYMBOL', '')
                        if symbol:
                            symbols.append({
                                'symbol': symbol + '.NS',
                                'company_name': row.get('NAME OF COMPANY', ''),
                                'sector': row.get('SERIES', ''),
                                'market_cap': None
                            })
                    
                    logger.info(f"Fetched {len(symbols)} symbols from {source_url}")
                    break
                    
            except Exception as e:
                logger.error(f"Failed to fetch from {source_url}: {e}")
                continue
        
        return symbols
    
    def get_all_nse_symbols(self, enrich_data: bool = True, validate: bool = False) -> List[Dict]:
        """
        Main method to get all NSE symbols using multiple approaches
        """
        logger.info("Starting NSE symbol fetch process...")
        
        # Try multiple methods in order of preference
        symbols = []
        
        # Method 1: NSE API
        if not symbols:
            symbols = self.fetch_nse_equity_list()
        
        # Method 2: CSV sources (if implemented)
        if not symbols:
            symbols = self.get_nse_symbols_from_csv_sources()
        
        # Method 3: Popular stocks fallback
        if not symbols:
            symbols = self._create_popular_stocks_list()
        
        if not symbols:
            logger.error("Failed to fetch any NSE symbols")
            return []
        
        logger.info(f"Initial symbol fetch completed: {len(symbols)} symbols")
        
        # Enrich with Yahoo Finance data
        if enrich_data:
            symbols = self.enrich_stock_data(symbols)
        
        # Validate symbols (optional - can be slow)
        if validate:
            symbols = self.validate_symbols(symbols)
        
        # Remove duplicates
        seen_symbols = set()
        unique_symbols = []
        for symbol in symbols:
            if symbol['symbol'] not in seen_symbols:
                seen_symbols.add(symbol['symbol'])
                unique_symbols.append(symbol)
        
        logger.info(f"Final NSE symbol list: {len(unique_symbols)} unique symbols")
        return unique_symbols