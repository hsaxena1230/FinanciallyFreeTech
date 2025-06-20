from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import logging
from config import Config

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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)