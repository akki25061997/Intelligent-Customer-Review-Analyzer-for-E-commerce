import boto3
import json
import os

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ReviewAnalysis')

sagemaker = boto3.client('sagemaker-runtime', region_name='us-east-1')

ENDPOINT = "jumpstart-dft-hf-summarization-dist-20251114-140145"

def lambda_handler(event, context):

    # 1️⃣ Read all reviews from DynamoDB
    response = table.scan()
    items = response.get('Items', [])

    if not items:
        return {"error": "No reviews found in DynamoDB"}

    # Combine only review text
    all_text = " ".join([item['ReviewText'] for item in items])

    # Avoid exceeding input limits
    if len(all_text) > 6000:
        all_text = all_text[:6000]

    # 2️⃣ CALL SAGEMAKER
    payload = {
        "text_inputs": all_text
    }

    try:
        sm_response = sagemaker.invoke_endpoint(
            EndpointName=ENDPOINT,
            ContentType="application/json",
            Body=json.dumps(payload)
        )
    except Exception as e:
        return {"error": f"SageMaker Error: {str(e)}"}

    # 3️⃣ Parse Output
    result = json.loads(sm_response['Body'].read().decode("utf-8"))

    summary = result.get("generated_text", "NO_SUMMARY_RETURNED")

    # 4️⃣ Save Summary
    table.put_item(Item={
        "ReviewID": "SUMMARY#ALL",
        "Summary": summary
    })

    return {
        "message": "Summary generated successfully!",
        "summary": summary
    }
