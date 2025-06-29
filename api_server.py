from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, timedelta
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

# BASIC STOCK DATA ENDPOINTS

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

# INDEX ENDPOINTS

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

# INDIVIDUAL STOCK ANALYSIS ENDPOINTS

@app.route('/api/stocks/by_sector_industry', methods=['GET'])
def get_stocks_by_sector_industry():
    """Get individual stocks within a specific sector-industry combination"""
    sector = request.args.get('sector')
    industry = request.args.get('industry')
    
    if not sector or not industry:
        return jsonify({
            'success': False,
            'error': 'Both sector and industry parameters are required'
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
            WHERE sector = %s AND industry = %s
            ORDER BY 
                CASE WHEN market_cap IS NOT NULL THEN market_cap ELSE 0 END DESC,
                CASE WHEN company_name IS NOT NULL AND company_name != '' 
                     THEN company_name 
                     ELSE symbol 
                END
        """, (sector, industry))
        
        stocks = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert to regular dict for JSON serialization
        stocks_list = []
        for stock in stocks:
            stocks_list.append({
                'symbol': stock['symbol'],
                'company_name': stock['company_name'],
                'sector': stock['sector'],
                'industry': stock['industry'],
                'market_cap': stock['market_cap']
            })
        
        return jsonify({
            'success': True,
            'data': {
                'stocks': stocks_list,
                'sector': sector,
                'industry': industry,
                'total_stocks': len(stocks_list)
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching stocks by sector-industry: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stocks/stage_analysis', methods=['GET'])
def get_stock_stage_analysis():
    """Get Weinstein stage analysis for individual stocks"""
    sector = request.args.get('sector')
    industry = request.args.get('industry')
    symbols = request.args.get('symbols')  # Comma-separated list of symbols
    
    if not ((sector and industry) or symbols):
        return jsonify({
            'success': False,
            'error': 'Either sector+industry or symbols parameter is required'
        }), 400
    
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get stocks to analyze
        if symbols:
            symbol_list = [s.strip() for s in symbols.split(',')]
            cursor.execute("""
                SELECT symbol, company_name, sector, industry, market_cap
                FROM stocks 
                WHERE symbol = ANY(%s)
                ORDER BY symbol
            """, (symbol_list,))
        else:
            cursor.execute("""
                SELECT symbol, company_name, sector, industry, market_cap
                FROM stocks 
                WHERE sector = %s AND industry = %s
                ORDER BY symbol
            """, (sector, industry))
        
        stocks = cursor.fetchall()
        
        if not stocks:
            return jsonify({
                'success': False,
                'error': 'No stocks found for the specified criteria'
            }), 404
        
        # Calculate stage analysis for each stock
        results = []
        
        for stock in stocks:
            try:
                # Get price history for the stock
                cursor.execute("""
                    SELECT time, close_price
                    FROM stock_prices 
                    WHERE symbol = %s 
                      AND time >= NOW() - INTERVAL '1 year'
                    ORDER BY time ASC
                """, (stock['symbol'],))
                
                price_data = cursor.fetchall()
                
                if len(price_data) >= 30:  # Need at least 30 data points
                    stage_analysis = calculate_stock_stage_analysis(price_data, stock)
                    stage_analysis['stock_info'] = dict(stock)
                    results.append(stage_analysis)
                else:
                    # Not enough data for analysis
                    results.append({
                        'stock_info': dict(stock),
                        'stage': 0,
                        'stage_description': 'Insufficient Data',
                        'stage_details': f'Only {len(price_data)} data points available',
                        'error': 'Insufficient price history for analysis'
                    })
                    
            except Exception as e:
                logger.error(f"Error analyzing {stock['symbol']}: {e}")
                results.append({
                    'stock_info': dict(stock),
                    'stage': 0,
                    'stage_description': 'Analysis Failed',
                    'stage_details': f'Error: {str(e)}',
                    'error': str(e)
                })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'stocks_analysis': results,
                'total_analyzed': len(results),
                'sector': sector,
                'industry': industry
            }
        })
        
    except Exception as e:
        logger.error(f"Error in stock stage analysis: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def calculate_stock_stage_analysis(price_data, stock_info):
    """Calculate Weinstein stage analysis for individual stock"""
    try:
        # Convert price data to lists
        dates = [row['time'] for row in price_data]
        prices = [float(row['close_price']) for row in price_data]
        
        if len(prices) < 30:
            raise ValueError("Insufficient price data")
        
        # Calculate 30-period moving average
        ma_period = min(30, len(prices) // 3)
        moving_average = []
        
        for i in range(ma_period - 1, len(prices)):
            ma_value = sum(prices[i - ma_period + 1:i + 1]) / ma_period
            moving_average.append(ma_value)
        
        # Get current values
        current_price = prices[-1]
        current_ma = moving_average[-1] if moving_average else prices[-1]
        previous_ma = moving_average[-2] if len(moving_average) > 1 else current_ma
        
        # Calculate trend characteristics
        price_vs_ma = 'above' if current_price > current_ma else 'below'
        ma_trend = 'rising' if current_ma > previous_ma else ('falling' if current_ma < previous_ma else 'flat')
        
        # Calculate recent performance and volatility
        recent_20 = prices[-20:] if len(prices) >= 20 else prices
        recent_high = max(recent_20)
        recent_low = min(recent_20)
        volatility = (recent_high - recent_low) / recent_low if recent_low > 0 else 0
        
        # Calculate 20-day performance
        performance_20 = 0
        if len(prices) >= 20:
            performance_20 = (current_price - prices[-20]) / prices[-20] * 100
        
        # Calculate trend strength
        trend_strength = abs(performance_20)
        
        # Determine Weinstein stage
        stage, stage_description, stage_details = determine_weinstein_stage(
            price_vs_ma, ma_trend, volatility, performance_20, current_price, recent_low
        )
        
        # Estimate stage duration
        stage_duration = estimate_stock_stage_duration(prices, moving_average)
        
        # Determine trend direction
        if performance_20 > 2:
            trend_direction = 'up'
        elif performance_20 < -2:
            trend_direction = 'down'
        else:
            trend_direction = 'sideways'
        
        return {
            'symbol': stock_info['symbol'],
            'stage': stage,
            'stage_description': stage_description,
            'stage_details': stage_details,
            'stage_duration': stage_duration,
            'trend_direction': trend_direction,
            'trend_strength': trend_strength,
            'current_price': round(current_price, 2),
            'moving_average': round(current_ma, 2),
            'price_vs_ma': price_vs_ma,
            'ma_trend': ma_trend,
            'performance_20_day': round(performance_20, 2),
            'volatility': round(volatility * 100, 2),
            'recent_high': round(recent_high, 2),
            'recent_low': round(recent_low, 2),
            'price_data': [{'time': dates[i].isoformat(), 'price': prices[i]} for i in range(len(prices))],
            'moving_average_data': moving_average
        }
        
    except Exception as e:
        logger.error(f"Error calculating stage analysis: {e}")
        raise

def determine_weinstein_stage(price_vs_ma, ma_trend, volatility, performance_20, current_price, recent_low):
    """Determine Weinstein stage based on technical indicators"""
    
    if price_vs_ma == 'below' and ma_trend == 'falling':
        if volatility < 0.05 and abs(performance_20) < 2:
            # Base building after decline
            return 1, 'Accumulation', 'Base building after decline, low volatility'
        else:
            # Still declining
            return 4, 'Declining', 'Downtrend continues, price below falling MA'
    
    elif price_vs_ma == 'above' and ma_trend == 'rising':
        if current_price > recent_low * 1.1:
            # Strong uptrend
            return 2, 'Advancing', 'Uptrend confirmed, price above rising MA'
        else:
            # Potential topping
            return 3, 'Distribution', 'Potential topping, momentum slowing'
    
    elif price_vs_ma == 'above' and ma_trend == 'flat':
        # Price above but MA flattening
        return 3, 'Distribution', 'Sideways action at highs, MA flattening'
    
    elif price_vs_ma == 'below' and ma_trend == 'flat':
        # Price below but MA stabilizing
        return 1, 'Accumulation', 'Potential base formation, MA stabilizing'
    
    else:
        # Transition phase
        if performance_20 > 0:
            return 2, 'Advancing', 'Potential breakout phase'
        else:
            return 4, 'Declining', 'Potential breakdown phase'

def estimate_stock_stage_duration(prices, moving_average):
    """Estimate how long the stock has been in current stage"""
    if len(moving_average) < 10:
        return 1
    
    # Simple estimation based on recent MA crossovers
    recent_periods = min(60, len(prices))
    recent_prices = prices[-recent_periods:]
    recent_ma = moving_average[-min(len(moving_average), recent_periods):]
    
    stage_changes = 0
    current_stage_periods = 0
    
    for i in range(1, min(len(recent_prices), len(recent_ma))):
        prev_condition = recent_prices[i-1] > recent_ma[i-1]
        curr_condition = recent_prices[i] > recent_ma[i]
        
        if prev_condition != curr_condition:
            stage_changes += 1
            current_stage_periods = 0
        current_stage_periods += 1
    
    return max(current_stage_periods, 1)

@app.route('/api/stocks/price_history_detailed', methods=['GET'])
def get_stock_price_history_detailed():
    """Get detailed price history for a specific stock with stage analysis"""
    symbol = request.args.get('symbol')
    days = int(request.args.get('days', 365))
    
    if not symbol:
        return jsonify({
            'success': False,
            'error': 'Symbol parameter is required'
        }), 400
    
    try:
        conn = stock_api.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get stock info
        cursor.execute("""
            SELECT symbol, company_name, sector, industry, market_cap
            FROM stocks 
            WHERE symbol = %s
        """, (symbol,))
        
        stock_info = cursor.fetchone()
        
        if not stock_info:
            return jsonify({
                'success': False,
                'error': 'Stock not found'
            }), 404
        
        # Get price history
        cursor.execute("""
            SELECT time, close_price
            FROM stock_prices 
            WHERE symbol = %s 
              AND time >= NOW() - INTERVAL '%s days'
            ORDER BY time ASC
        """, (symbol, days))
        
        price_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Calculate stage analysis if enough data
        stage_analysis = None
        if len(price_data) >= 30:
            try:
                stage_analysis = calculate_stock_stage_analysis(price_data, stock_info)
            except Exception as e:
                logger.error(f"Error calculating stage analysis for {symbol}: {e}")
        
        # Format price data for response
        formatted_price_data = []
        for record in price_data:
            formatted_price_data.append({
                'time': record['time'].isoformat(),
                'close_price': float(record['close_price'])
            })
        
        return jsonify({
            'success': True,
            'data': {
                'stock_info': dict(stock_info),
                'price_history': formatted_price_data,
                'stage_analysis': stage_analysis,
                'data_points': len(price_data)
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching detailed stock history for {symbol}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)