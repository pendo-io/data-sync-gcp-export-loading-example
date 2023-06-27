# This library of custom code snippets has been created by Pendo Professional Services, with the intent of enhancing the capabilities of Pendo products. Any and all snippets in this library are free and provided at no additional cost, and as such, are provided AS IS. For the avoidance of doubt, the library does not include any indemnification, support, or warranties of any kind, whether express or implied. For the avoidance of doubt, these snippets are outside of the remit of the Pendo Support team so please do not reach out to them for assistance with the library of code. Please do not reach out to Pendo Support for help with these snippets, as custom code is outside of the remit of their team and responsibilities.

import sys
import json
from google.cloud import storage

# Input arguments specifying which export to load and where to load it to
GCP_BUCKET = sys.argv[1] # Name of bucket containing data sync export (e.g. my-pendo-data-bucket)
GCP_PATH = sys.argv[2] # Path to manifest of interest in bucket (e.g. datasync/<SUBSCRIPTION_ID>/<APPLICATION_ID>)
NEW_COUNTER = int(sys.argv[3]) # New value to set counter to (int)

# Google cloud connections
STORAGE_CLIENT = storage.Client()
BUCKET = STORAGE_CLIENT.bucket(GCP_BUCKET)

# Save specified value to counter.json as int
try:
    print(f"Setting value in counter.json. New value for counter: {NEW_COUNTER}")
    blob = BUCKET.blob(f"{GCP_PATH}/counter.json")
    blob.upload_from_string(
        data=json.dumps({'count': NEW_COUNTER}),
        content_type='application/json'
    )
except Exception as e: 
    print(f"\tFailed setting counter.json. Exiting with exception: {str(e)}")
    sys.exit()