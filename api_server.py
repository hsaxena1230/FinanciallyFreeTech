from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime  # ← ADDED THIS IMPORT
from config import Config
from equiweighted_index import EquiweightedIndexGenerator

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockAPI:
    def __init__(self):
        self.config = Config()
    
    def get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(
            host=self.config.DB_HOST,
            port=self.config.DB_PORT,
            database=self.config.DB_NAME,
            user=self.config.DB_USER,
            password=self.config.DB_PASSWORD
        )

stock_api = StockAPI()

@app.route('/api/sectors', methods=['GET'])
def get_sectors():
    """Get all unique sectors"""
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cursor.execute("""
            SELECT DISTINCT sector
            FROM stocks 
            WHERE sector IS NOT NULL AND sector != ''
            ORDER BY sector
        """)
        
        sectors = [row['sector'] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': sectors
        })
        
    except Exception as e:
        logger.error(f"Error fetching sectors: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/industries', methods=['GET'])
def get_industries():
    """Get industries for a specific sector"""
    sector = request.args.get('sector')
    
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        if sector:
            cursor.execute("""
                SELECT DISTINCT industry
                FROM stocks 
                WHERE sector = %s 
                  AND industry IS NOT NULL 
                  AND industry != ''
                ORDER BY industry
            """, (sector,))
        else:
            cursor.execute("""
                SELECT DISTINCT industry
                FROM stocks 
                WHERE industry IS NOT NULL AND industry != ''
                ORDER BY industry
            """)
        
        industries = [row['industry'] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': industries
        })
        
    except Exception as e:
        logger.error(f"Error fetching industries: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/companies', methods=['GET'])
def get_companies():
    """Get companies filtered by sector and/or industry"""
    sector = request.args.get('sector')
    industry = request.args.get('industry')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 50))
    offset = (page - 1) * limit
    
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Build dynamic query based on filters
        where_conditions = []
        params = []
        
        if sector:
            where_conditions.append("sector = %s")
            params.append(sector)
        
        if industry:
            where_conditions.append("industry = %s")
            params.append(industry)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Get total count
        count_query = f"""
            SELECT COUNT(*) as total
            FROM stocks 
            {where_clause}
        """
        cursor.execute(count_query, params)
        total = cursor.fetchone()['total']
        
        # Get paginated results
        data_query = f"""
            SELECT 
                symbol,
                company_name,
                sector,
                industry,
                market_cap
            FROM stocks 
            {where_clause}
            ORDER BY 
                CASE WHEN company_name IS NOT NULL AND company_name != '' 
                     THEN company_name 
                     ELSE symbol 
                END
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(data_query, params + [limit, offset])
        companies = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'companies': companies,
                'pagination': {
                    'total': total,
                    'page': page,
                    'limit': limit,
                    'pages': (total + limit - 1) // limit
                }
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching companies: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get overall stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total_stocks,
                COUNT(CASE WHEN sector IS NOT NULL AND sector != '' THEN 1 END) as stocks_with_sector,
                COUNT(DISTINCT sector) as unique_sectors,
                COUNT(DISTINCT industry) as unique_industries
            FROM stocks
        """)
        stats = cursor.fetchone()
        
        # Get top sectors
        cursor.execute("""
            SELECT 
                sector,
                COUNT(*) as count
            FROM stocks 
            WHERE sector IS NOT NULL AND sector != ''
            GROUP BY sector 
            ORDER BY count DESC 
            LIMIT 10
        """)
        top_sectors = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'overall': dict(stats),
                'top_sectors': top_sectors
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/search', methods=['GET'])
def search_companies():
    """Search companies by name or symbol"""
    query = request.args.get('q', '').strip()
    limit = int(request.args.get('limit', 20))
    
    if not query or len(query) < 2:
        return jsonify({
            'success': False,
            'error': 'Query must be at least 2 characters'
        }), 400
    
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cursor.execute("""
            SELECT 
                symbol,
                company_name,
                sector,
                industry,
                market_cap
            FROM stocks 
            WHERE 
                symbol ILIKE %s 
                OR company_name ILIKE %s
            ORDER BY 
                CASE 
                    WHEN symbol ILIKE %s THEN 1
                    WHEN company_name ILIKE %s THEN 2
                    ELSE 3
                END,
                CASE WHEN company_name IS NOT NULL AND company_name != '' 
                     THEN company_name 
                     ELSE symbol 
                END
            LIMIT %s
        """, (f'%{query}%', f'%{query}%', f'{query}%', f'{query}%', limit))
        
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': results
        })
        
    except Exception as e:
        logger.error(f"Error searching companies: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/stock_history', methods=['GET'])
def get_stock_history():
    """Get historical price data for a specific stock"""
    symbol = request.args.get('symbol')
    days = int(request.args.get('days', 365))  # Default to 1 year of data
    
    if not symbol:
        return jsonify({
            'success': False,
            'error': 'Symbol parameter is required'
        }), 400
    
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Query to get price history, ordered by time
        cursor.execute("""
            SELECT 
                time,
                symbol,
                close_price
            FROM stock_prices 
            WHERE symbol = %s 
            ORDER BY time DESC
            LIMIT %s
        """, (symbol, days))
        
        price_history = cursor.fetchall()
        
        # Convert datetime objects to string format for JSON serialization
        for record in price_history:
            record['time'] = record['time'].isoformat()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': price_history
        })
        
    except Exception as e:
        logger.error(f"Error fetching stock history for {symbol}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stock_performance', methods=['GET'])
def get_stock_performance():
    """Get top and worst performing stocks based on 1-year price change"""
    sector = request.args.get('sector', '')
    industry = request.args.get('industry', '')
    limit = int(request.args.get('limit', 10))  # Default to 10 stocks
    period_days = int(request.args.get('period_days', 365))  # Default to 1 year
    
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Build dynamic query based on filters
        where_conditions = ["sp.time >= NOW() - INTERVAL '%s days'"]
        params = [period_days]
        
        if sector:
            where_conditions.append("s.sector = %s")
            params.append(sector)
        
        if industry:
            where_conditions.append("s.industry = %s")
            params.append(industry)
        
        where_clause = " AND ".join(where_conditions)
        
        # First, get all stocks with their oldest and most recent prices
        base_query = f"""
            WITH stock_prices_with_rank AS (
                SELECT 
                    sp.symbol,
                    sp.time,
                    sp.close_price,
                    ROW_NUMBER() OVER (PARTITION BY sp.symbol ORDER BY sp.time ASC) as oldest_rank,
                    ROW_NUMBER() OVER (PARTITION BY sp.symbol ORDER BY sp.time DESC) as newest_rank
                FROM stock_prices sp
                JOIN stocks s ON sp.symbol = s.symbol
                WHERE {where_clause}
            ),
            oldest_prices AS (
                SELECT symbol, close_price as oldest_price
                FROM stock_prices_with_rank
                WHERE oldest_rank = 1
            ),
            newest_prices AS (
                SELECT symbol, close_price as newest_price
                FROM stock_prices_with_rank
                WHERE newest_rank = 1
            ),
            price_changes AS (
                SELECT 
                    n.symbol,
                    n.newest_price,
                    o.oldest_price,
                    (n.newest_price - o.oldest_price) as absolute_change,
                    ((n.newest_price - o.oldest_price) / o.oldest_price * 100) as percent_change
                FROM newest_prices n
                JOIN oldest_prices o ON n.symbol = o.symbol
                WHERE o.oldest_price > 0  -- Prevent division by zero
            )
            SELECT 
                pc.symbol,
                s.company_name,
                s.sector,
                s.industry,
                pc.newest_price as current_price,
                pc.oldest_price as year_ago_price,
                pc.absolute_change,
                pc.percent_change,
                s.market_cap
            FROM price_changes pc
            JOIN stocks s ON pc.symbol = s.symbol
        """
        
        # Get top performing stocks (only profits)
        top_query = base_query + " WHERE pc.percent_change > 0 ORDER BY pc.percent_change DESC LIMIT %s"
        cursor.execute(top_query, params + [limit])
        top_performers = cursor.fetchall()
        
        # Get worst performing stocks (only losses)
        worst_query = base_query + " WHERE pc.percent_change < 0 ORDER BY pc.percent_change ASC LIMIT %s"
        cursor.execute(worst_query, params + [limit])
        worst_performers = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Format results
        for stock in top_performers + worst_performers:
            if 'percent_change' in stock:
                stock['percent_change'] = round(float(stock['percent_change']), 2)
            if 'absolute_change' in stock:
                stock['absolute_change'] = round(float(stock['absolute_change']), 2)
            if 'current_price' in stock:
                stock['current_price'] = round(float(stock['current_price']), 2)
            if 'year_ago_price' in stock:
                stock['year_ago_price'] = round(float(stock['year_ago_price']), 2)
        
        return jsonify({
            'success': True,
            'data': {
                'top_performers': top_performers,
                'worst_performers': worst_performers
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching stock performance: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Updated API endpoints for sector-industry indices only

@app.route('/api/indices/types', methods=['GET'])
def get_index_types():
    """Get all available index types - now only sector_industry"""
    try:
        return jsonify({
            'success': True,
            'data': ['sector_industry']  # Only one type now
        })
    except Exception as e:
        logger.error(f"Error getting index types: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/indices/names', methods=['GET'])
def get_index_names():
    """Get all available sector-industry index names"""
    try:
        generator = EquiweightedIndexGenerator()
        
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Only get sector_industry indices
        query = """
            SELECT DISTINCT index_name, index_type, MAX(constituent_count) as constituent_count
            FROM equiweighted_indices
            WHERE index_type = 'sector_industry'
            GROUP BY index_name, index_type 
            ORDER BY index_name
        """
        
        cursor.execute(query)
        indices = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': indices
        })
    
    except Exception as e:
        logger.error(f"Error getting index names: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/indices/data', methods=['GET'])
def get_index_data():
    """Get index data for plotting - only sector_industry type"""
    index_name = request.args.get('name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    if not index_name:
        return jsonify({
            'success': False,
            'error': 'Index name must be provided'
        }), 400
    
    try:
        generator = EquiweightedIndexGenerator()
        
        # Convert dates if provided
        start_date_obj = None
        if start_date:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        
        end_date_obj = None
        if end_date:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Get the data (always sector_industry type)
        df = generator.get_index_data(
            index_name=index_name, 
            index_type='sector_industry',
            start_date=start_date_obj,
            end_date=end_date_obj
        )
        
        if df.empty:
            return jsonify({
                'success': False,
                'error': 'No data found for the specified index'
            }), 404
        
        # Convert DataFrame to list of dictionaries
        result = []
        for _, row in df.iterrows():
            item = {
                'time': row['time'].isoformat() if hasattr(row['time'], 'isoformat') else str(row['time']),
                'index_name': row['index_name'],
                'index_type': row['index_type'],
                'index_value': float(row['index_value']),
                'constituent_count': int(row['constituent_count'])
            }
            result.append(item)
        
        return jsonify({
            'success': True,
            'data': result
        })
    
    except Exception as e:
        logger.error(f"Error getting index data: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/indices/generate', methods=['POST'])
def generate_indices():
    """Generate sector-industry indices (admin endpoint)"""
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # Convert dates if provided
        start_date_obj = None
        if start_date:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        
        end_date_obj = None
        if end_date:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Create the generator
        generator = EquiweightedIndexGenerator()
        
        # Create the table if it doesn't exist
        generator.create_index_table()
        
        # Start a background thread for generation
        import threading
        thread = threading.Thread(
            target=generator.generate_all_indices,
            args=(start_date_obj, end_date_obj)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Sector-industry index generation started in the background'
        })
    
    except Exception as e:
        logger.error(f"Error generating indices: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Add this new endpoint to list available sector-industry combinations
@app.route('/api/indices/sector_industry_combinations', methods=['GET'])
def get_sector_industry_combinations():
    """Get all available sector-industry combinations with stock counts"""
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cursor.execute("""
            SELECT 
                sector,
                industry,
                COUNT(*) as stock_count,
                CONCAT('SECTOR-INDUSTRY-', sector, '-', industry) as index_name
            FROM stocks 
            WHERE sector IS NOT NULL AND sector != '' 
              AND industry IS NOT NULL AND industry != '' 
            GROUP BY sector, industry
            HAVING COUNT(*) >= 3  -- Only combinations with at least 3 stocks
            ORDER BY sector, industry
        """)
        
        combinations = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': combinations
        })
        
    except Exception as e:
        logger.error(f"Error fetching sector-industry combinations: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    """Generate all indices (admin endpoint)"""
    # This should probably be protected by authentication
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # Convert dates if provided
        start_date_obj = None
        if start_date:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        
        end_date_obj = None
        if end_date:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Create the generator
        generator = EquiweightedIndexGenerator()
        
        # Create the table if it doesn't exist
        generator.create_index_table()
        
        # Start a background thread for generation
        # This is a long-running process, so we don't want to block the request
        import threading
        thread = threading.Thread(
            target=generator.generate_all_indices,
            args=(start_date_obj, end_date_obj)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Index generation started in the background'
        })
    
    except Exception as e:
        logger.error(f"Error generating indices: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
        
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)