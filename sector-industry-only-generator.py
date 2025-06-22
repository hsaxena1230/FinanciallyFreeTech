# equiweighted_index.py - Modified to only generate sector-industry indices
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from config import Config
from database import TimescaleDBManager
import psycopg2.extras
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EquiweightedIndexGenerator:
    """
    Class to generate equiweighted indices for sector-industry combinations only
    """
    
    def __init__(self):
        self.config = Config()
        self.db = TimescaleDBManager()
    
    def create_index_table(self):
        """Create the table to store index values if it doesn't exist"""
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return False
        
        try:
            with self.db.connection.cursor() as cursor:
                # Create the indices table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS equiweighted_indices (
                        time TIMESTAMPTZ NOT NULL,
                        index_name VARCHAR(100) NOT NULL,
                        index_type VARCHAR(20) NOT NULL,  -- Only 'sector_industry' now
                        index_value DECIMAL(16,4) NOT NULL,
                        constituent_count INTEGER NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (time, index_name)
                    );
                """)
                
                # Create hypertable
                cursor.execute("""
                    SELECT create_hypertable('equiweighted_indices', 'time', 
                                           if_not_exists => TRUE);
                """)
                
                # Create indices for better performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_equiweighted_indices_name 
                    ON equiweighted_indices (index_name);
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_equiweighted_indices_type 
                    ON equiweighted_indices (index_type);
                """)
                
                self.db.connection.commit()
                logger.info("Equiweighted indices table created successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error creating indices table: {e}")
            self.db.connection.rollback()
            return False
        finally:
            self.db.close()
    
    def generate_all_indices(self, start_date=None, end_date=None):
        """
        Generate equiweighted indices for sector-industry combinations only
        
        Args:
            start_date: Start date for index calculation (defaults to 1 year ago)
            end_date: End date for index calculation (defaults to today)
        """
        logger.info("Generating sector-industry combination indices only...")
        
        if start_date is None:
            start_date = datetime.now() - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now()
            
        # Only generate sector-industry combination indices
        self.generate_sector_industry_indices(start_date, end_date)
        
        logger.info("Sector-industry indices generated successfully")
    
    def generate_sector_industry_indices(self, start_date, end_date):
        """Generate equiweighted indices for each sector-industry combination"""
        logger.info("Generating sector-industry combination indices...")
        
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return False
        
        try:
            # Get all sector-industry combinations with minimum stock count
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT sector, industry, COUNT(*) as stock_count
                    FROM stocks 
                    WHERE sector IS NOT NULL AND sector != '' 
                      AND industry IS NOT NULL AND industry != '' 
                    GROUP BY sector, industry
                    HAVING COUNT(*) >= 3  -- Only combinations with at least 3 stocks
                    ORDER BY sector, industry
                """)
                combinations = cursor.fetchall()
            
            logger.info(f"Found {len(combinations)} sector-industry combinations with sufficient stocks")
            
            for sector, industry, stock_count in combinations:
                logger.info(f"Generating index for sector-industry: {sector}-{industry} ({stock_count} stocks)")
                self._generate_index_for_category("sector_industry", sector, industry, start_date, end_date)
                
            logger.info(f"Generated indices for {len(combinations)} sector-industry combinations")
            return True
            
        except Exception as e:
            logger.error(f"Error generating sector-industry indices: {e}")
            return False
        finally:
            self.db.close()
    
    def _generate_index_for_category(self, index_type, sector, industry, start_date, end_date):
        """
        Generate an equiweighted index for a sector-industry combination
        
        Args:
            index_type: Always 'sector_industry'
            sector: Sector name
            industry: Industry name
            start_date: Start date for index calculation
            end_date: End date for index calculation
        """
        # Connect to database
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return False
        
        try:
            # Create index name
            index_name = f"SECTOR-INDUSTRY-{sector}-{industry}"
            where_clause = "s.sector = %s AND s.industry = %s"
            params = [sector, industry]
            
            # 1. Get all stocks in this sector-industry combination
            with self.db.connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT s.symbol 
                    FROM stocks s 
                    WHERE {where_clause}
                """, params)
                stocks = [row[0] for row in cursor.fetchall()]
            
            if not stocks:
                logger.warning(f"No stocks found for {sector}-{industry}")
                return False
            
            if len(stocks) < 3:
                logger.warning(f"Only {len(stocks)} stocks found for {sector}-{industry}, skipping (minimum 3 required)")
                return False
            
            # 2. Get historical prices for these stocks
            price_data = self._get_historical_prices(stocks, start_date, end_date)
            
            if price_data.empty:
                logger.warning(f"No price data found for {sector}-{industry}")
                return False
            
            # 3. Calculate the equiweighted index
            index_values = self._calculate_equiweighted_index(price_data)
            
            if index_values.empty:
                logger.warning(f"Could not calculate index for {sector}-{industry}")
                return False
            
            # 4. Store the index in the database
            self._store_index_values(index_name, index_type, index_values, len(stocks))
            
            logger.info(f"Successfully generated index for {index_name} with {len(stocks)} constituents")
            return True
            
        except Exception as e:
            logger.error(f"Error generating index for {sector}-{industry}: {e}")
            return False
        finally:
            self.db.close()
    
    def _get_historical_prices(self, symbols, start_date, end_date):
        """
        Get historical prices for a list of symbols
        
        Args:
            symbols: List of stock symbols
            start_date: Start date for price data
            end_date: End date for price data
            
        Returns:
            DataFrame with price data
        """
        try:
            # Format dates for SQL
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Get price data
            with self.db.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT time, symbol, close_price
                    FROM stock_prices
                    WHERE symbol = ANY(%s)
                      AND time BETWEEN %s AND %s
                    ORDER BY time
                """, (symbols, start_date_str, end_date_str))
                
                rows = cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(rows)
            
            # Pivot to have symbols as columns
            pivot_df = df.pivot(index='time', columns='symbol', values='close_price')
            
            return pivot_df
            
        except Exception as e:
            logger.error(f"Error getting historical prices: {e}")
            return pd.DataFrame()
    
    def _calculate_equiweighted_index(self, price_data, base_value=1000):
        """
        Calculate an equiweighted index from price data
        
        Args:
            price_data: DataFrame with price data (time as index, symbols as columns)
            base_value: Starting value for the index (default: 1000)
            
        Returns:
            DataFrame with index values
        """
        try:
            # Remove stocks with too much missing data
            missing_ratio = price_data.isnull().sum() / len(price_data)
            valid_stocks = missing_ratio[missing_ratio < 0.5].index  # Keep stocks with <50% missing data
            
            if len(valid_stocks) < 3:
                logger.warning("Not enough stocks with sufficient data")
                return pd.DataFrame()
            
            # Filter to valid stocks only
            price_data_clean = price_data[valid_stocks]
            
            # Forward fill missing values
            price_data_clean = price_data_clean.fillna(method='ffill')
            
            # Drop rows where all stocks have missing data
            price_data_clean = price_data_clean.dropna(how='all')
            
            if price_data_clean.empty:
                logger.warning("No valid price data after cleaning")
                return pd.DataFrame()
            
            # Calculate returns for each stock
            returns = price_data_clean.pct_change().fillna(0)
            
            # Calculate the equiweighted index returns (average of all stock returns)
            equiweighted_returns = returns.mean(axis=1)
            
            # Calculate cumulative returns
            cum_returns = (1 + equiweighted_returns).cumprod()
            
            # Create the index series starting at base_value
            index_values = base_value * cum_returns
            
            # Create a DataFrame with the index values
            index_df = pd.DataFrame({
                'time': index_values.index,
                'index_value': index_values.values
            })
            
            return index_df
            
        except Exception as e:
            logger.error(f"Error calculating equiweighted index: {e}")
            return pd.DataFrame()
    
    def _store_index_values(self, index_name, index_type, index_values, constituent_count):
        """
        Store index values in the database
        
        Args:
            index_name: Name of the index
            index_type: Type of index (always 'sector_industry')
            index_values: DataFrame with index values
            constituent_count: Number of stocks in the index
        """
        if index_values.empty:
            logger.warning(f"No index values to store for {index_name}")
            return False
        
        try:
            # Insert index values in batches
            with self.db.connection.cursor() as cursor:
                for _, row in index_values.iterrows():
                    cursor.execute("""
                        INSERT INTO equiweighted_indices
                        (time, index_name, index_type, index_value, constituent_count)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (time, index_name) 
                        DO UPDATE SET 
                            index_value = EXCLUDED.index_value,
                            constituent_count = EXCLUDED.constituent_count
                    """, (
                        row['time'], 
                        index_name, 
                        index_type, 
                        float(row['index_value']), 
                        constituent_count
                    ))
                
                self.db.connection.commit()
                logger.info(f"Stored {len(index_values)} index values for {index_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error storing index values for {index_name}: {e}")
            self.db.connection.rollback()
            return False
    
    def get_index_data(self, index_name=None, index_type=None, start_date=None, end_date=None):
        """
        Get index data from the database
        
        Args:
            index_name: Name of the index (optional)
            index_type: Type of index (always 'sector_industry')
            start_date: Start date for data (optional)
            end_date: End date for data (optional)
            
        Returns:
            DataFrame with index data
        """
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return pd.DataFrame()
        
        try:
            query = """
                SELECT time, index_name, index_type, index_value, constituent_count
                FROM equiweighted_indices
                WHERE index_type = 'sector_industry'
            """
            params = []
            
            if index_name:
                query += " AND index_name = %s"
                params.append(index_name)
            
            if start_date:
                query += " AND time >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND time <= %s"
                params.append(end_date)
            
            query += " ORDER BY time"
            
            with self.db.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(rows)
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting index data: {e}")
            return pd.DataFrame()
        finally:
            self.db.close()
    
    def get_available_indices(self):
        """Get list of all available sector-industry indices"""
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return []
        
        try:
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT index_name, MAX(constituent_count) as constituent_count
                    FROM equiweighted_indices
                    WHERE index_type = 'sector_industry'
                    GROUP BY index_name
                    ORDER BY index_name
                """)
                indices = cursor.fetchall()
            
            return [{'index_name': name, 'constituent_count': count} for name, count in indices]
            
        except Exception as e:
            logger.error(f"Error getting available indices: {e}")
            return []
        finally:
            self.db.close()

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate equiweighted indices for sector-industry combinations only')
    parser.add_argument('--create-table', action='store_true', help='Create the indices table in the database')
    parser.add_argument('--generate', action='store_true', help='Generate all sector-industry indices')
    parser.add_argument('--start-date', type=str, help='Start date for index calculation (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date for index calculation (YYYY-MM-DD)')
    parser.add_argument('--list-indices', action='store_true', help='List all available indices')
    
    args = parser.parse_args()
    
    # Parse dates if provided
    start_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    
    end_date = None
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    # Create the index generator
    generator = EquiweightedIndexGenerator()
    
    # Create the table if requested
    if args.create_table:
        generator.create_index_table()
    
    # Generate indices if requested
    if args.generate:
        generator.generate_all_indices(start_date, end_date)
    
    # List indices if requested
    if args.list_indices:
        indices = generator.get_available_indices()
        print(f"\nAvailable Sector-Industry Indices ({len(indices)}):")
        print("-" * 60)
        for idx in indices:
            print(f"{idx['index_name']} ({idx['constituent_count']} stocks)")
