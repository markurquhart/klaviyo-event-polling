import os
import requests
import time
import logging

from dotenv import load_dotenv

# Initialize and configure logging
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format, filename="app.log")

# Add a console handler for logging
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format))
logging.getLogger().addHandler(console_handler)

# Constants and configurations
load_dotenv()

# Polling interval in seconds
POLLING_INTERVAL = 3

# Batch size for initial load
INITIAL_LOAD_BATCH_SIZE = 200

# Klaviyo API URLs and Keys
KLA_BASE_URL = "https://a.klaviyo.com/api/events/?include=profile&sort=timestamp"
KLA_PUB_KEY = os.getenv('PUB-API-KEY')
KLA_PRI_KEY = os.getenv('PRI-API-KEY')
WEBHOOK_SITE = os.getenv('WEBHOOK-URL')
LOG_FILE = "last_processed_timestamp.txt"

HEADERS = {
    "accept": "application/json",
    "revision": "2023-09-15",
    "Authorization": f"Klaviyo-API-Key {KLA_PRI_KEY}"
}

logging.info("Script started.")

# Function to retrieve Klaviyo events with optional timestamp filter
def get_klaviyo_events(start_timestamp=None):
    events = []
    url = KLA_BASE_URL
    if start_timestamp:
        url += f"&filter=greater-than(timestamp,{start_timestamp})"
    while True:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            events_data = data.get('data', [])
            for event in events_data:
                event_timestamp = event.get('attributes', {}).get('timestamp')
                if event_timestamp:
                    events.append(event)
            next_page_cursor = data.get('links', {}).get('next')
            if not next_page_cursor:
                break
            url = next_page_cursor
        else:
            logging.error(f"Error fetching events. Status code: {response.status_code}")
            logging.error(f"Response content: {response.text}")
            response.raise_for_status()
    
    return events

# Function to send data to a webhook
def send_to_webhook(data):
    response = requests.post(WEBHOOK_SITE, json=data)
    if response.status_code != 200:
        logging.error(f"Error sending to webhook: {response.text}")
        response.raise_for_status()
    else:
        logging.info(f"Data sent to webhook successfully. Number of events: {len(data)}")

# Function to get the last processed timestamp
def get_last_processed_timestamp():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            timestamp = f.read().strip()
            return timestamp
    return None

# Function to update the last processed timestamp
def update_last_processed_timestamp(timestamp):
    with open(LOG_FILE, "w") as f:
        f.write(str(timestamp))  # Convert the timestamp to a string

# Function to handle the initial load of events in batches
def handle_large_initial_load():
    last_timestamp = get_last_processed_timestamp()
    events = []
    while True:
        batch = get_klaviyo_events(last_timestamp)[:INITIAL_LOAD_BATCH_SIZE]
        if not batch:
            break
        events.extend(batch)
        last_event_timestamp = batch[-1].get('attributes', {}).get('timestamp')
        if last_event_timestamp:
            last_timestamp = last_event_timestamp
            send_to_webhook(batch)
            update_last_processed_timestamp(last_event_timestamp)
    return last_timestamp

last_timestamp = get_last_processed_timestamp()

if not last_timestamp:
    logging.info("Handling initial load...")
    last_timestamp = handle_large_initial_load()

logging.info("Entering the polling loop.")

# Poll every POLLING_INTERVAL seconds for new events
while True:
    try:
        last_timestamp = get_last_processed_timestamp()
        events = get_klaviyo_events(last_timestamp)
        
        # Check if we have any new events
        if events:
            logging.info(f"Received {len(events)} events.")
            send_to_webhook(events)
            last_event_timestamp = events[-1].get('attributes', {}).get('timestamp')
            if last_event_timestamp:
                update_last_processed_timestamp(last_event_timestamp)
                logging.info(f"Processed {len(events)} new events. Last event timestamp: {last_event_timestamp}")
            else:
                logging.warning("Last event timestamp not found in the response.")
        else:
            logging.info("No new events found.")
            
        time.sleep(POLLING_INTERVAL)
    except Exception as e:
        logging.error(f"Error occurred: {e}")
