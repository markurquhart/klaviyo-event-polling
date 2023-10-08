import os
import requests
import time
import logging

from dotenv import load_dotenv

# Logging Config
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format, filename="app.log")

# More Logging Config
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format))
logging.getLogger().addHandler(console_handler)

# Call DOTENV
load_dotenv()

# How fast should the polling run, in seconds
POLLING_INTERVAL = 3

# How many max events should we send per batch? Important for initial load
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

# Cheeky logging commments
logging.info("Script started. Get ready for fun!")

def get_klaviyo_events(start_timestamp=None):
    # Start with an empty payloads object
    payloads = []
    url = KLA_BASE_URL
    # Build URL for GET request, using a filter of a timestamp if available
    if start_timestamp:
        url += f"&filter=greater-than(timestamp,{start_timestamp})"
    # Loop will run as long as there is a 'next page' cursor    
    while True:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            # Append to payload object
            payload = response.json()
            if payload.get('data'):
                payloads.append(payload)
            # Check for the next page cursor
            next_page_cursor = payload.get('links', {}).get('next')
            if not next_page_cursor:
                break
            url = next_page_cursor
        else:
            logging.error(f"Error fetching events. Status code: {response.status_code}")
            logging.error(f"Response content: {response.text}")
            response.raise_for_status()

    return payloads

def send_to_webhook(data):
    response = requests.post(WEBHOOK_SITE, json=data)
    if response.status_code != 200:
        logging.error(f"Error sending to webhook: {response.text}")
        response.raise_for_status()
    else:
        logging.info(f"Events sent successfully: {len(data['data'])} events")

def get_last_processed_timestamp():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            timestamp = f.read().strip()
            return timestamp
    return None

def update_last_processed_timestamp(timestamp):
    with open(LOG_FILE, "w") as f:
        f.write(str(timestamp))

# Poll every POLLING_INTERVAL seconds for new events
while True:
    try:
        last_timestamp = get_last_processed_timestamp()
        all_payloads = get_klaviyo_events(last_timestamp)
        
        for payload in all_payloads:
            logging.info(f"Found {len(payload['data'])} new events.")
            send_to_webhook(payload)

            # Update the last processed timestamp from the last event in the payload
            last_event_timestamp = payload['data'][-1].get('attributes', {}).get('timestamp')
            if last_event_timestamp:
                update_last_processed_timestamp(last_event_timestamp)
                logging.info(f"Processed payload. Last event timestamp: {last_event_timestamp}")
            else:
                logging.warning("Last event timestamp not found in the payload.")
            
        logging.info("No new events. Sad panda.")
        time.sleep(POLLING_INTERVAL)
    except Exception as e:
        logging.error(f"Error occurred: {e}")

