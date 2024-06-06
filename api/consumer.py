import os
import django
import pika
import json
import requests
from io import BytesIO
from PIL import Image
from django.core.files.base import ContentFile

# Set the Django settings module before importing Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'api.settings')  # Replace with your actual settings module
django.setup()

from listings.models import Listing

# RabbitMQ connection parameters
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='image_queue', durable=True)

def callback(ch, method, properties, body):
    data = json.loads(body)
    image_url = data['image_url']
    
    try:
        response = requests.get(image_url)
        response.raise_for_status()  # Raise an error for non-200 status codes
        
        image_content = response.content
        image_bytes = BytesIO(image_content)
        
        # Example: Using Pillow (PIL) for resizing
        image = Image.open(image_bytes)
        # Resize image if needed
        # image = image.resize((width, height), Image.ANTIALIAS)
        
        # Save to Listing object
        listing = Listing.objects.create(image_url=image_url)
        listing.image.save(image_url.split('/')[-1], ContentFile(image_content), save=True)
        
        print(f"Image saved for URL: {image_url}")
    
    except requests.RequestException as e:
        print(f"Failed to download image from URL: {image_url}. Error: {e}")
    except Exception as e:
        print(f"Error processing image from URL: {image_url}. Error: {e}")

# Set up RabbitMQ consumer
channel.basic_consume(queue='image_queue', on_message_callback=callback, auto_ack=True)
print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
