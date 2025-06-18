import requests
import pandas as pd
import logging
import time
import json
from typing import List, Dict, Optional
from datetime import datetime
import io

logger = logging.getLogger(__name__)

class DynamicNSEFetcher:
    """
    Dynamic NSE Symbol Fetcher that gets real-time stock lists from multiple sources
    """
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://www.nseindia.com',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Initialize session with NSE homepage to get cookies
        try:
            self.session.get('https://www.nseindia.com', timeout=10)
        except:
            pass
    
    def fetch_from_nsetools(self) -> List[Dict]:
        """
        Fetch all individual NSE stock symbols using nsetools library
        """
        try:
            from nsetools import Nse
            nse = Nse()
            
            logger.info("Fetching individual stock codes using nsetools...")
            stock_codes = nse.get_stock_codes()
            
            symbols = []
            for symbol, company_name in stock_codes.items():
                # Only include individual stocks, not indices
                if not self._is_index_symbol(symbol):
                    symbols.append({
                        'symbol': symbol + '.NS',
                        'company_name': company_name,
                        'sector': '',
                        'market_cap': None
                    })
            
            logger.info(f"Fetched {len(symbols)} individual stock symbols using nsetools")
            return symbols
            
        except Exception as e:
            logger.error(f"Error fetching from nsetools: {e}")
            return []
    
    def fetch_from_nifty_indices(self) -> List[Dict]:
        """
        Fetch individual stocks from various Nifty indices (stock constituents only)
        """
        symbols = []
        indices_urls = {
            'NIFTY 50 Stocks': 'https://www1.nseindia.com/content/indices/ind_nifty50list.csv',
            'NIFTY NEXT 50 Stocks': 'https://www1.nseindia.com/content/indices/ind_niftynext50list.csv',
            'NIFTY 100 Stocks': 'https://www1.nseindia.com/content/indices/ind_nifty100list.csv',
            'NIFTY 200 Stocks': 'https://www1.nseindia.com/content/indices/ind_nifty200list.csv',
            'NIFTY 500 Stocks': 'https://www1.nseindia.com/content/indices/ind_nifty500list.csv',
            'NIFTY MIDCAP 50 Stocks': 'https://www1.nseindia.com/content/indices/ind_niftymidcap50list.csv',
            'NIFTY MIDCAP 100 Stocks': 'https://www1.nseindia.com/content/indices/ind_niftymidcap100list.csv',
            'NIFTY SMALLCAP 100 Stocks': 'https://www1.nseindia.com/content/indices/ind_niftysmallcap100list.csv',
        }
        
        for index_name, url in indices_urls.items():
            try:
                logger.info(f"Fetching individual stocks from {index_name}")
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    df = pd.read_csv(io.StringIO(response.text))
                    
                    # Clean column names (remove spaces and make consistent)
                    df.columns = df.columns.str.strip()
                    
                    # Find symbol column (different indices might have different column names)
                    symbol_col = None
                    for col in df.columns:
                        if 'symbol' in col.lower() or col.lower() in ['symbol', 'stock']:
                            symbol_col = col
                            break
                    
                    if symbol_col:
                        for _, row in df.iterrows():
                            symbol = str(row[symbol_col]).strip()
                            # Only include individual stock symbols, not indices
                            if symbol and symbol != 'nan' and not self._is_index_symbol(symbol):
                                company_name = ''
                                sector = ''
                                
                                # Try to get company name from different possible columns
                                for col in df.columns:
                                    if 'company' in col.lower() or 'name' in col.lower():
                                        company_name = str(row[col]).strip()
                                        break
                                
                                # Try to get sector information
                                for col in df.columns:
                                    if 'sector' in col.lower() or 'industry' in col.lower():
                                        sector = str(row[col]).strip()
                                        break
                                
                                symbols.append({
                                    'symbol': symbol + '.NS',
                                    'company_name': company_name,
                                    'sector': sector,
                                    'market_cap': None
                                })
                        
                        logger.info(f"Fetched {len(df)} individual stocks from {index_name}")
                else:
                    logger.warning(f"Failed to fetch {index_name}: HTTP {response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Error fetching {index_name}: {e}")
                continue
            
            # Rate limiting
            time.sleep(1)
        
        # Remove duplicates
        unique_symbols = {}
        for stock in symbols:
            unique_symbols[stock['symbol']] = stock
        
        result = list(unique_symbols.values())
        logger.info(f"Total unique individual stock symbols from indices: {len(result)}")
        return result
    
    def _is_index_symbol(self, symbol: str) -> bool:
        """
        Check if a symbol represents an index rather than an individual stock
        """
        index_keywords = [
            'NIFTY', 'SENSEX', 'BSE', 'INDEX', 'CNX', 'BANK', 'IT', 'AUTO', 
            'PHARMA', 'METAL', 'ENERGY', 'FMCG', 'REALTY', 'MEDIA', 'PSU',
            'MIDCAP', 'SMALLCAP', 'INFRASTRUCTURE', 'DIVIDEND', 'QUALITY',
            'MOMENTUM', 'ALPHA', 'COMMODITIES', 'CONSUMPTION', 'CPSE'
        ]
        
        symbol_upper = symbol.upper()
        
        # Check if symbol contains index keywords
        for keyword in index_keywords:
            if keyword in symbol_upper:
                return True
        
        # Additional checks for common index patterns
        if (symbol_upper.startswith('CNX') or 
            symbol_upper.startswith('NIFTY') or
            'INDEX' in symbol_upper):
            return True
            
        return False
    
    def fetch_from_nse_api(self) -> List[Dict]:
        """
        Fetch individual stocks using NSE's internal APIs
        """
        symbols = []
        
        try:
            # NSE search API that can give us stock symbols
            logger.info("Attempting to fetch individual stocks from NSE API...")
            
            # Get cookies first
            self.session.get('https://www.nseindia.com', timeout=10)
            
            # Try equity master API
            api_url = 'https://www.nseindia.com/api/equity-master'
            response = self.session.get(api_url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                for stock in data:
                    if isinstance(stock, dict) and 'symbol' in stock:
                        symbol = stock['symbol']
                        # Only include individual stocks, not indices
                        if not self._is_index_symbol(symbol):
                            symbols.append({
                                'symbol': symbol + '.NS',
                                'company_name': stock.get('companyName', ''),
                                'sector': stock.get('industry', ''),
                                'market_cap': None
                            })
                
                logger.info(f"Fetched {len(symbols)} individual stock symbols from NSE API")
                return symbols
            
        except Exception as e:
            logger.warning(f"NSE API method failed: {e}")
        
        return symbols
    
    def fetch_from_bhavcopy(self) -> List[Dict]:
        """
        Fetch individual stock symbols from NSE Bhavcopy (daily trading data)
        """
        symbols = []
        
        try:
            # NSE Bhavcopy URL (equity segment)
            logger.info("Fetching individual stocks from NSE Bhavcopy...")
            
            # Try to get latest bhavcopy
            from datetime import datetime, timedelta
            
            # Try last few days to get a working bhavcopy
            for days_back in range(0, 10):
                date = datetime.now() - timedelta(days=days_back)
                # Skip weekends
                if date.weekday() >= 5:
                    continue
                
                date_str = date.strftime('%d%m%Y')
                bhavcopy_url = f'https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv'
                
                try:
                    response = self.session.get(bhavcopy_url, timeout=20)
                    
                    if response.status_code == 200:
                        df = pd.read_csv(io.StringIO(response.text))
                        
                        # Filter for equity (EQ) series only - individual stocks
                        eq_stocks = df[df['SERIES'] == 'EQ'] if 'SERIES' in df.columns else df
                        
                        for _, row in eq_stocks.iterrows():
                            symbol = str(row['SYMBOL']).strip()
                            # Only include individual stock symbols, not indices
                            if symbol and symbol != 'nan' and not self._is_index_symbol(symbol):
                                symbols.append({
                                    'symbol': symbol + '.NS',
                                    'company_name': '',  # Bhavcopy doesn't have company names
                                    'sector': '',
                                    'market_cap': None
                                })
                        
                        logger.info(f"Fetched {len(symbols)} individual stock symbols from Bhavcopy dated {date_str}")
                        break
                        
                except Exception as e:
                    continue
            
        except Exception as e:
            logger.warning(f"Bhavcopy method failed: {e}")
        
        return symbols
    
    def fetch_from_external_apis(self) -> List[Dict]:
        """
        Fetch from external APIs that provide NSE data
        """
        symbols = []
        
        # Try different external APIs
        external_sources = [
            {
                'name': 'RapidAPI NSE',
                'url': 'https://latest-stock-price.p.rapidapi.com/any',
                'headers': {
                    'X-RapidAPI-Key': 'demo',  # Users would need to get their own key
                    'X-RapidAPI-Host': 'latest-stock-price.p.rapidapi.com'
                }
            }
        ]
        
        for source in external_sources:
            try:
                logger.info(f"Trying {source['name']}...")
                response = requests.get(
                    source['url'], 
                    headers=source.get('headers', {}),
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    # Process data based on API structure
                    # This would need to be customized based on actual API response
                    
            except Exception as e:
                logger.warning(f"{source['name']} failed: {e}")
                continue
        
        return symbols
    
    def get_all_nse_stocks_dynamically(self) -> List[Dict]:
        """
        Main method to get all NSE stocks using multiple dynamic sources
        """
        logger.info("Starting dynamic NSE stock fetch...")
        all_symbols = []
        
        # Method 1: Use nsetools library (most reliable)
        logger.info("=== Method 1: nsetools library ===")
        nsetools_symbols = self.fetch_from_nsetools()
        if nsetools_symbols:
            all_symbols.extend(nsetools_symbols)
            logger.info(f"✓ nsetools: {len(nsetools_symbols)} symbols")
        else:
            logger.warning("✗ nsetools: Failed")
        
        # Method 2: Fetch from Nifty indices (reliable for major stocks)
        logger.info("=== Method 2: Nifty Indices ===")
        indices_symbols = self.fetch_from_nifty_indices()
        if indices_symbols:
            all_symbols.extend(indices_symbols)
            logger.info(f"✓ Nifty Indices: {len(indices_symbols)} symbols")
        else:
            logger.warning("✗ Nifty Indices: Failed")
        
        # Method 3: Try NSE API
        logger.info("=== Method 3: NSE API ===")
        api_symbols = self.fetch_from_nse_api()
        if api_symbols:
            all_symbols.extend(api_symbols)
            logger.info(f"✓ NSE API: {len(api_symbols)} symbols")
        else:
            logger.warning("✗ NSE API: Failed")
        
        # Method 4: Try Bhavcopy
        logger.info("=== Method 4: NSE Bhavcopy ===")
        bhavcopy_symbols = self.fetch_from_bhavcopy()
        if bhavcopy_symbols:
            all_symbols.extend(bhavcopy_symbols)
            logger.info(f"✓ Bhavcopy: {len(bhavcopy_symbols)} symbols")
        else:
            logger.warning("✗ Bhavcopy: Failed")
        
        # Remove duplicates while preserving company names and sectors
        unique_symbols = {}
        for stock in all_symbols:
            symbol = stock['symbol']
            if symbol not in unique_symbols:
                unique_symbols[symbol] = stock
            else:
                # Update with better information if available
                existing = unique_symbols[symbol]
                if not existing['company_name'] and stock['company_name']:
                    existing['company_name'] = stock['company_name']
                if not existing['sector'] and stock['sector']:
                    existing['sector'] = stock['sector']
        
        final_symbols = list(unique_symbols.values())
        
        # Sort by symbol for consistency
        final_symbols.sort(key=lambda x: x['symbol'])
        
        logger.info(f"=== FINAL RESULT ===")
        logger.info(f"Total unique NSE symbols fetched dynamically: {len(final_symbols)}")
        
        if final_symbols:
            logger.info("✅ Dynamic NSE fetch SUCCESSFUL")
            return final_symbols
        else:
            logger.error("❌ All dynamic methods failed")
            return []
    
    def test_symbol_validity(self, symbols: List[Dict], sample_size: int = 10) -> float:
        """
        Test a sample of symbols to check validity
        """
        if not symbols:
            return 0.0
        
        sample = symbols[:min(sample_size, len(symbols))]
        valid_count = 0
        
        logger.info(f"Testing {len(sample)} symbols for validity...")
        
        for stock in sample:
            try:
                import yfinance as yf
                ticker = yf.Ticker(stock['symbol'])
                hist = ticker.history(period="1d")
                
                if not hist.empty:
                    valid_count += 1
                    logger.debug(f"✓ {stock['symbol']} - Valid")
                else:
                    logger.debug(f"✗ {stock['symbol']} - No data")
                    
            except Exception as e:
                logger.debug(f"✗ {stock['symbol']} - Error: {e}")
            
            time.sleep(0.1)  # Rate limiting
        
        validity_rate = (valid_count / len(sample)) * 100
        logger.info(f"Validity test: {valid_count}/{len(sample)} symbols valid ({validity_rate:.1f}%)")
        
        return validity_rate