import json
from datetime import datetime, timezone

def handler(event, context):
    '''
    Handle API requests for stock data enrichment
    '''
    
    symbol = event.get('pathParameters', {}).get('symbol', 'UNKNOWN')
    
    # Mock enrichment data
    enrichment_data = {
        'symbol': symbol,
        'company_name': f'{symbol} Corporation',
        'sector': 'Technology',
        'market_cap': '1B+',
        'enriched_at': datetime.now(tz=timezone.utc).isoformat()
    }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(enrichment_data)
    }