import os
import io
from flask import Flask, request
from google.cloud import storage
from PIL import Image
from cloudevents.http import from_http

app = Flask(__name__)
storage_client = storage.Client()

PROCESSED_BUCKET = os.environ.get('PROCESSED_BUCKET')

@app.route('/', methods=['POST'])
def process_image():
    # Parse CloudEvent
    event = from_http(request.headers, request.get_data())
    
    # Get event data
    data = event.get_data()
    bucket_name = data['bucket']
    file_name = data['name']
    
    print(f"Processing image: gs://{bucket_name}/{file_name}")
    
    # Skip if not an image
    if not file_name.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
        print(f"Skipping non-image file: {file_name}")
        return ('OK', 200)
    
    try:
        # Download the image
        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(file_name)
        image_bytes = source_blob.download_as_bytes()
        
        # Process the image
        image = Image.open(io.BytesIO(image_bytes))
        
        # Resize to thumbnail (300x300)
        image.thumbnail((300, 300))
        
        # Convert to RGB if needed (for JPEG)
        if image.mode in ('RGBA', 'LA', 'P'):
            image = image.convert('RGB')
        
        # Save to bytes
        output = io.BytesIO()
        image.save(output, format='JPEG', quality=85)
        output.seek(0)
        
        # Upload to processed bucket
        processed_bucket = storage_client.bucket(PROCESSED_BUCKET)
        processed_blob = processed_bucket.blob(f"thumbnail_{file_name}")
        processed_blob.upload_from_file(output, content_type='image/jpeg')
        
        print(f"Successfully processed: thumbnail_{file_name}")
        return ('OK', 200)
        
    except Exception as e:
        print(f"Error processing image: {str(e)}")
        return (f'Error: {str(e)}', 500)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
