import boto3
import csv
import json
from decimal import Decimal

# Initialize AWS clients
s3 = boto3.client('s3')
comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ReviewAnalysis')

def lambda_handler(event, context):
    print("Lambda triggered with event:", json.dumps(event))
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print(f"Processing file: {key} from bucket: {bucket}")

    # Read CSV file
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(data)
    print("CSV columns detected:", reader.fieldnames)

    count = 0
    for row in reader:
        review_text = row.get('Review Text', '').strip()
        if not review_text:
            print("⚠️ Skipping empty review row:", row)
            continue

        try:
            # Sentiment Analysis
            sentiment = comprehend.detect_sentiment(Text=review_text, LanguageCode='en')
            key_phrases = comprehend.detect_key_phrases(Text=review_text, LanguageCode='en')

            # Convert float -> Decimal for DynamoDB
            sentiment_score_decimal = {
                k: Decimal(str(v)) for k, v in sentiment['SentimentScore'].items()
            }

            # Prepare DynamoDB item
            item = {
                'ReviewID': str(hash(review_text))[:10],
                'ReviewText': review_text,
                'Rating': str(row.get('Rating', 'N/A')),
                'DetectedSentiment': sentiment['Sentiment'],
                'SentimentScore': sentiment_score_decimal,
                'KeyPhrases': [p['Text'] for p in key_phrases['KeyPhrases']],
                'ProductCategory': row.get('Department Name', 'Unknown')
            }

            # Save item
            table.put_item(Item=item)
            count += 1
            print(f"✅ Saved ReviewID {item['ReviewID']} successfully")

        except Exception as e:
            print(f"❌ Error processing review: {e}")
            continue

    print(f"✅ Lambda completed. Total reviews saved: {count}")
    return {'status': 'Processed successfully', 'count': count}
