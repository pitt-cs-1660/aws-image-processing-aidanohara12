import json
from PIL import Image
import io
import boto3
from pathlib import Path
from urllib.parse import unquote_plus

def download_from_s3(bucket, key):
    s3 = boto3.client('s3')
    buffer = io.BytesIO()
    s3.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    return Image.open(buffer)

def upload_to_s3(bucket, key, data, content_type='image/jpeg'):
    s3 = boto3.client('s3')
    if isinstance(data, Image.Image):
        buffer = io.BytesIO()
        data.save(buffer, format='JPEG')
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket, key)
    else:
        s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

def resize_handler(event, context):
    print("Resize Lambda triggered")
    processed_count = 0
    failed_count = 0

    for sns_record in event.get('Records', []):
        try:
            sns_message = json.loads(sns_record['Sns']['Message'])
            for s3_event in sns_message.get('Records', []):
                try:
                    s3_record = s3_event['s3']
                    bucket_name = s3_record['bucket']['name']
                    object_key = unquote_plus(s3_record['object']['key'])

                    print(f"Processing: s3://{bucket_name}/{object_key}")

                    img = download_from_s3(bucket_name, object_key)
                    img = img.convert("RGB") if img.mode != "RGB" else img

                    run_id = Path(object_key).stem.split("test-")[-1]
                    resized_key = f"processed/resize/test-{run_id}.jpg"

                    resized = img.copy()
                    resized.thumbnail((512, 512))
                    upload_to_s3(bucket_name, resized_key, resized)

                    print(f"Uploaded resized image to s3://{bucket_name}/{resized_key}")
                    processed_count += 1

                except Exception as e:
                    failed_count += 1
                    print(f"Failed to process {object_key}: {str(e)}")

        except Exception as e:
            failed_count += 1
            print(f"Failed to process SNS record: {str(e)}")

    summary = {'statusCode': 200 if failed_count == 0 else 207,
               'processed': processed_count,
               'failed': failed_count}
    print(f"Processing complete: {processed_count} succeeded, {failed_count} failed")
    return summary