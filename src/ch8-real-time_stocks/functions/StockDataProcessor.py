import json
import boto3
import base64
from datetime import datetime
from logging import getLogger

logger = getLogger(__name__)
logger.setLevel('INFO')
firehose = boto3.client('firehose')

def handler(event, context):
    '''
    Process stock data from MSK and send to Firehose
    '''
    records = []
    
    for record in event.get('records', []):
        # Decode Kafka message
        payload = base64.b64decode(record['value']).decode('utf-8')
        stock_data = json.loads(payload)
        
        # Enrich with timestamp
        enriched_data = {
            **stock_data,
            'processed_timestamp': datetime.utcnow().isoformat(),
            'partition_key': stock_data.get('symbol', 'unknown')
        }
        
        records.append({
            'Data': json.dumps(enriched_data) + '\\n'
        })
    
    # Send to Firehose
    if records:
        firehose.put_record_batch(
            DeliveryStreamName='realtime-stocks-firehose',
            Records=records
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Processed {len(records)} records')
    }