# equiweighted_index.py
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
    Class to generate equiweighted indices for sector and industry combinations
    and store them in the database
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
                        index_type VARCHAR(20) NOT NULL,  -- 'sector', 'industry', or 'sector_industry'
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
        Generate equiweighted indices for all sectors, industries and their combinations
        
        Args:
            start_date: Start date for index calculation (defaults to 1 year ago)
            end_date: End date for index calculation (defaults to today)
        """
        logger.info("Generating all equiweighted indices...")
        
        if start_date is None:
            start_date = datetime.now() - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now()
            
        # 1. Generate sector indices
        self.generate_sector_indices(start_date, end_date)
        
        # 2. Generate industry indices
        self.generate_industry_indices(start_date, end_date)
        
        # 3. Generate sector-industry combination indices
        self.generate_sector_industry_indices(start_date, end_date)
        
        logger.info("All indices generated successfully")
    
    def generate_sector_indices(self, start_date, end_date):
        """Generate equiweighted indices for each sector"""
        logger.info("Generating sector indices...")
        
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return False
        
        try:
            # Get all sectors
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT sector 
                    FROM stocks 
                    WHERE sector IS NOT NULL AND sector != '' 
                    ORDER BY sector
                """)
                sectors = [row[0] for row in cursor.fetchall()]
            
            for sector in sectors:
                logger.info(f"Generating index for sector: {sector}")
                self._generate_index_for_category("sector", sector, None, start_date, end_date)
                
            logger.info(f"Generated indices for {len(sectors)} sectors")
            return True
            
        except Exception as e:
            logger.error(f"Error generating sector indices: {e}")
            return False
        finally:
            self.db.close()
    
    def generate_industry_indices(self, start_date, end_date):
        """Generate equiweighted indices for each industry"""
        logger.info("Generating industry indices...")
        
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return False
        
        try:
            # Get all industries
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT industry 
                    FROM stocks 
                    WHERE industry IS NOT NULL AND industry != '' 
                    ORDER BY industry
                """)
                industries = [row[0] for row in cursor.fetchall()]
            
            for industry in industries:
                logger.info(f"Generating index for industry: {industry}")
                self._generate_index_for_category("industry", None, industry, start_date, end_date)
                
            logger.info(f"Generated indices for {len(industries)} industries")
            return True
            
        except Exception as e:
            logger.error(f"Error generating industry indices: {e}")
            return False
        finally:
            self.db.close()
    
    def generate_sector_industry_indices(self, start_date, end_date):
        """Generate equiweighted indices for each sector-industry combination"""
        logger.info("Generating sector-industry combination indices...")
        
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return False
        
        try:
            # Get all sector-industry combinations
            with self.db.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT sector, industry 
                    FROM stocks 
                    WHERE sector IS NOT NULL AND sector != '' 
                      AND industry IS NOT NULL AND industry != '' 
                    ORDER BY sector, industry
                """)
                combinations = cursor.fetchall()
            
            for sector, industry in combinations:
                logger.info(f"Generating index for sector-industry: {sector}-{industry}")
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
        Generate an equiweighted index for a given category
        
        Args:
            index_type: Type of index ('sector', 'industry', or 'sector_industry')
            sector: Sector name (or None for industry-only indices)
            industry: Industry name (or None for sector-only indices)
            start_date: Start date for index calculation
            end_date: End date for index calculation
        """
        # Connect to database
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return False
        
        try:
            # Determine the index name and query conditions
            if index_type == "sector":
                index_name = f"SECTOR-{sector}"
                where_clause = "s.sector = %s"
                params = [sector]
            elif index_type == "industry":
                index_name = f"INDUSTRY-{industry}"
                where_clause = "s.industry = %s"
                params = [industry]
            else:  # sector_industry
                index_name = f"SECTOR-INDUSTRY-{sector}-{industry}"
                where_clause = "s.sector = %s AND s.industry = %s"
                params = [sector, industry]
            
            # 1. Get all stocks in this category
            with self.db.connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT s.symbol 
                    FROM stocks s 
                    WHERE {where_clause}
                """, params)
                stocks = [row[0] for row in cursor.fetchall()]
            
            if not stocks:
                logger.warning(f"No stocks found for {index_type}: {sector if sector else ''} {industry if industry else ''}")
                return False
            
            # 2. Get historical prices for these stocks
            price_data = self._get_historical_prices(stocks, start_date, end_date)
            
            if price_data.empty:
                logger.warning(f"No price data found for {index_type}: {sector if sector else ''} {industry if industry else ''}")
                return False
            
            # 3. Calculate the equiweighted index
            index_values = self._calculate_equiweighted_index(price_data)
            
            # 4. Store the index in the database
            self._store_index_values(index_name, index_type, index_values, len(stocks))
            
            logger.info(f"Successfully generated index for {index_name} with {len(stocks)} constituents")
            return True
            
        except Exception as e:
            logger.error(f"Error generating index for {index_type}: {sector if sector else ''} {industry if industry else ''}: {e}")
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
            # Calculate returns for each stock
            returns = price_data.pct_change().fillna(0)
            
            # Calculate the equiweighted index returns
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
            index_type: Type of index ('sector', 'industry', or 'sector_industry')
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
            index_type: Type of index (optional)
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
                WHERE 1=1
            """
            params = []
            
            if index_name:
                query += " AND index_name = %s"
                params.append(index_name)
            
            if index_type:
                query += " AND index_type = %s"
                params.append(index_type)
            
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
    
    def plot_indices(self, index_names=None, index_type=None, start_date=None, end_date=None, 
                     save_dir="index_charts", save_format="png"):
        """
        Plot indices and save the charts
        
        Args:
            index_names: List of index names to plot (optional)
            index_type: Type of index to plot (optional)
            start_date: Start date for data (optional)
            end_date: End date for data (optional)
            save_dir: Directory to save charts (default: "index_charts")
            save_format: Format to save charts (default: "png")
        """
        # Create save directory if it doesn't exist
        os.makedirs(save_dir, exist_ok=True)
        
        # Get index data
        if index_names:
            # Plot each index separately
            for index_name in index_names:
                df = self.get_index_data(index_name=index_name, start_date=start_date, end_date=end_date)
                if not df.empty:
                    self._plot_single_index(df, save_dir, save_format)
        elif index_type:
            # Plot all indices of this type on the same chart
            df = self.get_index_data(index_type=index_type, start_date=start_date, end_date=end_date)
            if not df.empty:
                self._plot_multiple_indices(df, index_type, save_dir, save_format)
        else:
            # Plot all types of indices
            for type_name in ["sector", "industry", "sector_industry"]:
                df = self.get_index_data(index_type=type_name, start_date=start_date, end_date=end_date)
                if not df.empty:
                    self._plot_multiple_indices(df, type_name, save_dir, save_format)
    
    def _plot_single_index(self, df, save_dir, save_format):
        """Plot a single index and save the chart"""
        index_name = df['index_name'].iloc[0]
        
        # Convert time to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'])
        
        # Create the plot
        plt.figure(figsize=(12, 6))
        plt.plot(df['time'], df['index_value'])
        
        # Format the plot
        plt.title(f"Equiweighted Index: {index_name}")
        plt.xlabel("Date")
        plt.ylabel("Index Value")
        plt.grid(True, alpha=0.3)
        
        # Format the date on the x-axis
        plt.gca().xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))
        plt.xticks(rotation=45)
        
        # Add annotation with constituent count
        constituent_count = df['constituent_count'].iloc[0]
        plt.annotate(f"Constituents: {constituent_count}", 
                     xy=(0.02, 0.95), xycoords='axes fraction',
                     bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
        
        plt.tight_layout()
        
        # Save the plot
        safe_name = index_name.replace("/", "-").replace(" ", "_")
        plt.savefig(f"{save_dir}/{safe_name}.{save_format}")
        plt.close()
        
        logger.info(f"Saved chart for {index_name}")
    
    def _plot_multiple_indices(self, df, index_type, save_dir, save_format):
        """Plot multiple indices on the same chart"""
        # Convert time to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['time']):
            df['time'] = pd.to_datetime(df['time'])
        
        # Get unique index names
        index_names = df['index_name'].unique()
        
        if len(index_names) > 10:
            # If there are too many indices, plot them in separate charts
            logger.info(f"Too many indices ({len(index_names)}) to plot on one chart, plotting top 10 by constituent count")
            
            # Get the top 10 indices by constituent count
            top_indices = df.drop_duplicates('index_name').nlargest(10, 'constituent_count')['index_name'].tolist()
            
            # Filter the data for these indices
            df_filtered = df[df['index_name'].isin(top_indices)]
            
            # Pivot the data
            pivot_df = df_filtered.pivot(index='time', columns='index_name', values='index_value')
            
            # Plot the data
            plt.figure(figsize=(14, 8))
            pivot_df.plot(figsize=(14, 8), ax=plt.gca())
            
            # Format the plot
            plt.title(f"Top 10 Equiweighted Indices by Constituent Count - Type: {index_type}")
            plt.xlabel("Date")
            plt.ylabel("Index Value")
            plt.grid(True, alpha=0.3)
            plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
            
            # Format the date on the x-axis
            plt.gca().xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))
            plt.xticks(rotation=45)
            
            plt.tight_layout()
            
            # Save the plot
            plt.savefig(f"{save_dir}/{index_type}_top10.{save_format}")
            plt.close()
            
            logger.info(f"Saved chart for top 10 {index_type} indices")
        else:
            # Pivot the data
            pivot_df = df.pivot(index='time', columns='index_name', values='index_value')
            
            # Plot the data
            plt.figure(figsize=(14, 8))
            pivot_df.plot(figsize=(14, 8), ax=plt.gca())
            
            # Format the plot
            plt.title(f"Equiweighted Indices - Type: {index_type}")
            plt.xlabel("Date")
            plt.ylabel("Index Value")
            plt.grid(True, alpha=0.3)
            plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
            
            # Format the date on the x-axis
            plt.gca().xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))
            plt.xticks(rotation=45)
            
            plt.tight_layout()
            
            # Save the plot
            plt.savefig(f"{save_dir}/{index_type}_all.{save_format}")
            plt.close()
            
            logger.info(f"Saved chart for all {index_type} indices")

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate equiweighted indices for sectors and industries')
    parser.add_argument('--create-table', action='store_true', help='Create the indices table in the database')
    parser.add_argument('--generate', action='store_true', help='Generate all indices')
    parser.add_argument('--start-date', type=str, help='Start date for index calculation (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date for index calculation (YYYY-MM-DD)')
    parser.add_argument('--plot', action='store_true', help='Plot indices')
    parser.add_argument('--index-name', type=str, help='Name of the index to plot')
    parser.add_argument('--index-type', type=str, choices=['sector', 'industry', 'sector_industry'], 
                        help='Type of index to plot')
    parser.add_argument('--save-dir', type=str, default='index_charts', help='Directory to save charts')
    parser.add_argument('--save-format', type=str, default='png', help='Format to save charts')
    
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
    
    # Plot indices if requested
    if args.plot:
        if args.index_name:
            generator.plot_indices(index_names=[args.index_name], start_date=start_date, end_date=end_date,
                                 save_dir=args.save_dir, save_format=args.save_format)
        elif args.index_type:
            generator.plot_indices(index_type=args.index_type, start_date=start_date, end_date=end_date,
                                 save_dir=args.save_dir, save_format=args.save_format)
        else:
            generator.plot_indices(start_date=start_date, end_date=end_date,
                                 save_dir=args.save_dir, save_format=args.save_format)
