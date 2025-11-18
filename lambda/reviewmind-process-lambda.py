import boto3
from decimal import Decimal
import json
import time

# DynamoDB + Comprehend + S3 clients
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('reviewmind-reviews')

comprehend = boto3.client('comprehend')
s3 = boto3.client('s3')

# 🔴 IMPORTANT: this must be YOUR bucket name (no folder here)
# For you it is: reviewmind-data-souhardo
OUTPUT_BUCKET = "reviewmind-data-souhardo"


def lambda_handler(event, context):

    for record in event["Records"]:

        # We only care about INSERT or MODIFY events
        if record["eventName"] not in ["INSERT", "MODIFY"]:
            continue

        new_image = record["dynamodb"]["NewImage"]

        review_id = new_image["review_id"]["S"]
        review_text = new_image["review_text"]["S"]

        # Skip if already processed
        if new_image.get("analysis_status", {}).get("S") == "COMPLETED":
            continue

        # ---------- 1. Run Comprehend ----------
        result = comprehend.detect_sentiment(
            Text=review_text,
            LanguageCode='en'
        )

        sentiment_label = result["Sentiment"]           # e.g. "POSITIVE"
        scores = result["SentimentScore"]               # dict of floats

        # ---------- 2. Convert scores for DynamoDB ----------
        sentiment_scores_ddb = {
            "Positive": Decimal(str(scores["Positive"])),
            "Negative": Decimal(str(scores["Negative"])),
            "Neutral":  Decimal(str(scores["Neutral"])),
            "Mixed":    Decimal(str(scores["Mixed"]))
        }

        # ---------- 3. Update DynamoDB item ----------
        table.update_item(
            Key={"review_id": review_id},
            UpdateExpression="""
                SET
                    analysis_status = :s,
                    sentiment       = :sent,
                    sentiment_score = :scr
            """,
            ExpressionAttributeValues={
                ":s":    "COMPLETED",
                ":sent": sentiment_label,
                ":scr":  sentiment_scores_ddb
            }
        )

        # ---------- 4. Prepare FLATTENED output for S3 ----------
        #  (THIS was the main problem earlier)
        output_record = {
            "review_id": review_id,
            "review_text": review_text,
            "sentiment": sentiment_label,
            "sentiment_score.Positive": float(scores["Positive"]),
            "sentiment_score.Negative": float(scores["Negative"]),
            "sentiment_score.Neutral":  float(scores["Neutral"]),
            "sentiment_score.Mixed":    float(scores["Mixed"]),
            "timestamp": int(time.time())
        }

        # ---------- 5. Save JSON to S3 ----------
        file_name = f"processed/review_{review_id}.json"

        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=file_name,
            Body=json.dumps(output_record),
            ContentType="application/json"
        )

    return {"status": "OK"}
