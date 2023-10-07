# Python script for polling events
This script does an initial request to get all events from your Klaviyo instance, and then batches them to your endpoint in size of 200 events per request. After that, the script listens for new events every X minute and sends those deltas to the endpoint you defined.

### State management
The script produces two files to manage state, they are stored locally in the project folder: 
- app.log
    - output of our logging 
- last_processed_timestamp.txt
    - maintains our filtering criteria for net new poll requests to the Klaviyo api

