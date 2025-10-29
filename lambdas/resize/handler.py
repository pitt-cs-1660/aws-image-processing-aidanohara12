import json
from PIL import Image
import io
import boto3
from pathlib import Path

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
    """
    Resize Lambda - Process all images in the event
    """
    print("Resize Lambda triggered")
    print(f"Event received with {len(event.get('Records', []))} SNS records")

    processed_count = 0
    failed_count = 0

    # iterate over all SNS records
    for sns_record in event.get('Records', []):
        try:
            # extract and parse SNS message
            sns_message = json.loads(sns_record['Sns']['Message'])

            # iterate over all S3 records in the SNS message
            for s3_event in sns_message.get('Records', []):
                try:
                    s3_record = s3_event['s3']
                    bucket_name = s3_record['bucket']['name']
                    object_key = s3_record['object']['key']

                    print(f"Processing: s3://{bucket_name}/{object_key}")

                    ######
                    #
                    #  TODO: add resize lambda code here
                    #
                    ######
                    from urllib.parse import unquote_plus
                    from PIL import ImageOps, ExifTags

                    decoded_key = unquote_plus(object_key)

                    img = download_from_s3(bucket_name, decoded_key)

                    img = ImageOps.exif_transpose(img)
                    if img.mode not in ("RGB", "L"):
                        img = img.convert("RGB")

                    name = Path(decoded_key).stem
                    resized_key = f"processed/resized/{name}.jpg"
                    grayscale_key = f"processed/grayscale/{name}.jpg"
                    metadata_key = f"processed/metadata/{name}.json"

                    resized = img.copy()
                    resized.thumbnail((1024, 1024))
                    upload_to_s3(bucket_name, resized_key, resized, content_type="image/jpeg")
                    print(f"Uploaded resized image to s3://{bucket_name}/{resized_key}")

                    gray = (img.convert("L")
                              if img.mode != "L" else img.copy())
                    upload_to_s3(bucket_name, grayscale_key, gray, content_type="image/jpeg")
                    print(f"Uploaded grayscale image to s3://{bucket_name}/{grayscale_key}")

                    exif_dict = {}
                    try:
                        raw_exif = img.getexif()
                        if raw_exif:
                            tag_map = {ExifTags.TAGS.get(k, str(k)): raw_exif.get(k) for k in raw_exif.keys()}
                            # Convert non-JSON-serializable values to strings
                            exif_dict = {k: (v if isinstance(v, (str, int, float, bool, type(None))) else str(v))
                                         for k, v in tag_map.items()}
                    except Exception as ex:
                        print(f"No EXIF extracted for {decoded_key}: {ex}")

                    upload_to_s3(
                        bucket_name,
                        metadata_key,
                        json.dumps({"source_key": decoded_key, "exif": exif_dict}).encode("utf-8"),
                        content_type="application/json"
                    )
                    print(f"Uploaded metadata to s3://{bucket_name}/{metadata_key}")

                    processed_count += 1

                except Exception as e:
                    failed_count += 1
                    error_msg = f"Failed to process {object_key}: {str(e)}"
                    print(error_msg)

        except Exception as e:
            print(f"Failed to process SNS record: {str(e)}")
            failed_count += 1

    summary = {
        'statusCode': 200 if failed_count == 0 else 207,  # @note: 207 = multi-status
        'processed': processed_count,
        'failed': failed_count,
    }

    print(f"Processing complete: {processed_count} succeeded, {failed_count} failed")
    return summary
