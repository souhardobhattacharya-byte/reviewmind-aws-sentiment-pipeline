import boto3
import csv
import uuid

dynamodb = boto3.resource('dynamodb')

# IMPORTANT: Updated to new table name
table = dynamodb.Table('reviewmind-reviews-new')

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get bucket and file from S3 trigger
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print("Reading file:", key)

    # Read CSV from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(content)

    # Process each row
    for row in reader:

        # Extract review_id
        review_id = row.get("review_id")

        # Generate unique ID if review_id missing
        if not review_id or review_id.strip() == "":
            review_id = str(uuid.uuid4())

        # Insert cleaned item (NO review_date)
        item = {
            "review_id": review_id,
            "app_name": row.get("app_name", "Unknown"),
            "review_text": row.get("review_text", ""),
            "rating": row.get("rating", "0"),
            "analysis_status": "PENDING"
        }

        try:
            table.put_item(Item=item)
            print("Inserted:", item)
        except Exception as e:
            print("Error inserting row:", e)

    return {
        "status": "SUCCESS",
        "file": key
    }
