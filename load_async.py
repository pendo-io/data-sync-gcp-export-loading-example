# This library of custom code snippets has been created by Pendo Professional Services, with the intent of enhancing the capabilities of Pendo products. Any and all snippets in this library are free and provided at no additional cost, and as such, are provided AS IS. For the avoidance of doubt, the library does not include any indemnification, support, or warranties of any kind, whether express or implied. For the avoidance of doubt, these snippets are outside of the remit of the Pendo Support team so please do not reach out to them for assistance with the library of code. Please do not reach out to Pendo Support for help with these snippets, as custom code is outside of the remit of their team and responsibilities.

import sys
import json
import time
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound

# Input arguments specifying which export to load and where to load it to
GCP_BUCKET = sys.argv[1] # Name of bucket containing data sync export (e.g. my-pendo-data-bucket)
GCP_PATH = sys.argv[2] # Path to manifest of interest in bucket (e.g. datasync/<SUBSCRIPTION_ID>/<APPLICATION_ID>)
GCP_PROJECT = sys.argv[3] # Name of project to load data to in BigQuery (e.g. my-reporting-project)
GCP_DATASET = sys.argv[4] # Name of dataset to load data to in BigQuery (e.g. my-pendo-dataset)

# Google cloud connections
STORAGE_CLIENT = storage.Client()
BUCKET = STORAGE_CLIENT.bucket(GCP_BUCKET)
BIGQUERY_CLIENT = bigquery.Client()
DATASET = None

# Files/values read from cloud storage
COUNTER = None # Global counter from cloud storage indicating what export to load
FINAL_COUNTER = None # Final value counter should be iterated to after successful run (initial value of COUNTER + MAX_EXPORTS_TO_LOAD)
MANIFEST = None # Manifest content read from cloud storage
EXPORT = None # Current export based on counter from manifest
ROOT_URL = None # Root url for export to build full path to all files loaded below

# Global config
MAX_NUM_TRIES = 3 # Maximum number of tries to load file before exiting program
MAX_THREADS = 16 # Max number of threads to spread load jobs between in async mode
MAX_EXPORTS_TO_LOAD = 30 # Max number of exports program will load in a single run. 
                        # Can be adjusted based on volume of exports generated
                        # e.g. Running hourly with max exports set to 100, up to 2400 exports will be processed daily

# Async processing globals
THREAD_EXECUTOR = ThreadPoolExecutor(MAX_THREADS) # Executor to run async tasks in allocated threads
JOBS_STARTED = 0 # Number of jobs started, to track when all async jobs have finished
JOBS_FINISHED = 0 # Number of jobs finished, to track when all async jobs have finished

print(f"Loading Pendo data from {GCP_BUCKET}{GCP_PATH} to project {GCP_PROJECT}, dataset {GCP_DATASET}")

# Read specified JSON file from cloud storage
def read_json(blob_name):
    blob = BUCKET.blob(blob_name)
    with blob.open('r') as f:
        return json.loads(blob.download_as_string(client=None))
    
# Submit job to thread executor upon creation and add one to number of jobs started
def start_job(job_obj):
    global JOBS_STARTED, THREAD_EXECUTOR
    JOBS_STARTED += 1
    future = THREAD_EXECUTOR.submit(async_job, job_obj)
    future.add_done_callback(job_finished)
    return

# Validate result of job upon finish and add one to number of jobs finished
def job_finished(future):
    global JOBS_FINISHED
    JOBS_FINISHED += 1
    job = future.result()
    validate_async(job)
    return

# Function to run async GCP jobs required for loading tables
def async_job(job_obj):
    if (job_obj['type'] == 'load'):
        print(f"Running load from {job_obj['params']['uri']} to {job_obj['params']['table_id']}")
        job = BIGQUERY_CLIENT.load_table_from_uri(job_obj['params']['uri'], job_obj['params']['table_id'], job_config=job_obj['params']['job_config'])
    elif (job_obj['type'] == 'query'):
        print(f"Started query {job_obj['params']['query']}")
        job = BIGQUERY_CLIENT.query(job_obj['params']['query'])

    return { 
        'result': job.result(), 
        'job': job, 
        'job_obj': job_obj,
        'next': job_obj['next']
    }

# Validate that async job has finished and has no next steps
# Return True if job is done with no next steps
def validate_async(async_job):
    print(async_job)
    print(async_job['job_obj'])
    if (async_job['job_obj']['type'] == 'load'):
        print(f"Finished load from {async_job['job_obj']['params']['uri']} to {async_job['job_obj']['params']['table_id']}")
    elif (async_job['job_obj']['type'] == 'query'):
        print(f"Finished query {async_job['job_obj']['params']['query']}")


    # If errors and attempts left, retry.
    # Else if errors and no attempts left, exit.
    if (async_job['job'].errors != None):
        if (async_job['attempt_number'] <= MAX_NUM_TRIES):
            print(f"\tRetrying job {async_job['job']} with errors {async_job['job'].errors}. (Attempt {async_job['job_obj']['attempt_number']})")
            async_job['job_obj']['attempt_number'] = async_job['job_obj']['attempt_number'] + 1
            start_job(async_job['job_obj'])
            return False
        else:
            print(f"\tJob {async_job['job']} failed with errors {async_job['job'].errors} after {async_job['job_obj']['attempt_number']} attempts. Exiting.")
            sys.exit()

    # If there are other async steps to follow, kick the next step off
    if (async_job['next'] != None):
        print(f"\tNext job: {async_job['next']}")
        start_job(async_job['next'])
        return False

    print('\tNo next job in series')
    return True

# Load array of definitions tables
# Iterate over all definition files for relevant table and create queue of jobs to execute in order 
#  (e.g. Load allpages-000.avro, then allpages-001.avro, then allpages-001.avro)
def load_definitions_tables(definition_files, table_name):
    # Build job for each file in array and put in array
    job_list = []
    for i,definition_file in enumerate(definition_files):
            print(f"\t\tCreating load job for: {definition_file}")

            job_list.append({
                'type': 'load',
                'params': {
                    'uri': f"{ROOT_URL}/{definition_file}",
                    'table_id': f"{GCP_PROJECT}.{GCP_DATASET}.{table_name}",
                    'job_config': bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.AVRO, # Specify Avro file format 
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if i == 0 else bigquery.WriteDisposition.WRITE_APPEND, # Truncate for first file in array, otherwise append
                        use_avro_logical_types=True # Convert Avro logical types to BQ types (e.g. TIMESTAMP) rather than raw types (e.g. INTEGER)
                    ),
                },
                'attempt_number': 1,
                'next': None
            })
    
    # Build single job object from array of jobs
    job = job_list[0]
    current_job = job
    for i in range(1, len(job_list)):
        current_job['next'] = job_list[i]
        current_job = current_job['next']

    # Start job queue
    print(f"\t\t\tStarting job queue for table {table_name}")
    start_job(job)
    return

# Load event tables from array
# If table already exists, drop data for period from partitioned table and load new data from array of files
# If table does not exist, create temp table from first file, create partitioned table, drop temp table, and load new data from remaining array of files
def load_events_tables(event_files, period_id, table_name): # uri, table_id, period_id
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO, # Specify Avro file format 
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Always append events
        use_avro_logical_types=True # Convert Avro logical types to BQ types (e.g. TIMESTAMP) rather than raw types (e.g. INTEGER)
    )

    try:
        # Check if table exists
        BIGQUERY_CLIENT.get_table(f"{GCP_PROJECT}.{GCP_DATASET}.{table_name}")
        print(f"\t\tTable {GCP_PROJECT}.{GCP_DATASET}.{table_name} already exists.")

        # First job is deleting previous data for period
        print(f"\t\t\tCreating delete job for {period_id} from {GCP_PROJECT}.{GCP_DATASET}.{table_name}")
        job_list = [{
            'type': 'query',
            'params': {
                'query': f"DELETE FROM `{GCP_PROJECT}.{GCP_DATASET}.{table_name}` WHERE periodId = PARSE_DATE('%Y%m%d',  '{period_id}')"
            },
            'attempt_number': 1,
            'next': None
        }] 

        # Build job for each file in array of files and put in array
        for i,event_file in enumerate(event_files):
            print(f"\t\tCreating load job for: {event_file}")

            job_list.append({
                'type': 'load',
                'params': {
                    'uri': f"{ROOT_URL}/{event_file}",
                    'table_id': f"{GCP_PROJECT}.{GCP_DATASET}.{table_name}",
                    'job_config': job_config,
                },
                'attempt_number': 1,
                'next': None
            })
    
        # Build single job object from array of jobs
        job = job_list[0]
        current_job = job
        for i in range(1, len(job_list)):
            current_job['next'] = job_list[i]
            current_job = current_job['next']

        # Start job queue
        print(f"\t\t\tStarting job queue for table {table_name}")
        start_job(job)
    except NotFound:
        # First job is creating temp table from first file in array
        # Second job is creating partitioned table from temp
        # Third job is dropping temp table
        print(f"\t\t\tCreating load job for {GCP_PROJECT}.{GCP_DATASET}.{table_name}_temp from {ROOT_URL}/{event_files[0]}")
        print(f"\t\t\tCreating partition table job for {GCP_PROJECT}.{GCP_DATASET}.{table_name}")
        print(f"\t\t\tCreating drop table job for {GCP_PROJECT}.{GCP_DATASET}.{table_name}_temp")
        job_list = [
            {
                'type': 'load',
                'params': {
                    'uri': f"{ROOT_URL}/{event_files[0]}",
                    'table_id': f"{GCP_PROJECT}.{GCP_DATASET}.{table_name}_temp",
                    'job_config': job_config,
                },
                'attempt_number': 1,
                'next': None
            },
            {
                'type': 'query',
                'params': {
                    'query': f"CREATE TABLE `{GCP_PROJECT}.{GCP_DATASET}.{table_name}` PARTITION BY periodId AS SELECT * FROM `{GCP_PROJECT}.{GCP_DATASET}.{table_name}_temp`;"
                },
                'attempt_number': 1,
                'next': None
            },
            {
                'type': 'query',
                'params': {
                    'query': f"DROP TABLE `{GCP_PROJECT}.{GCP_DATASET}.{table_name}_temp`"
                },
                'attempt_number': 1,
                'next': None
            }
        ] 

        # Build load job for each remaining file in array of files and put in array
        for i,event_file in enumerate(event_files):
            if (i != 0):
                print(f"\t\t\tCreating load job for: {event_file}")
                job_list.append({
                    'type': 'load',
                    'params': {
                        'uri': f"{ROOT_URL}/{event_file}",
                        'table_id': f"{GCP_PROJECT}.{GCP_DATASET}.{table_name}",
                        'job_config': job_config,
                    },
                    'attempt_number': 1,
                    'next': None
                })
    
        # Build single job object from array of jobs
        job = job_list[0]
        current_job = job
        for i in range(1, len(job_list)):
            current_job['next'] = job_list[i]
            current_job = current_job['next']

        # Start job queue
        print(f"\t\t\tStarting job queue for table {table_name}")
        start_job(job)
    return

# Perform upfront setup to ensure we are ready to load export
# 1 - Verify counter file is present, if not create
# 2 - Verify dataset is present, if not create
# 3 - Load manifest and store as global for parsing in load functions
def setup():
    global COUNTER, FINAL_COUNTER, DATASET, MANIFEST, EXPORT, ROOT_URL # Globals defined as a part of setup

    # 1 - Verify counter file is present, if not create
    try: 
        COUNTER = read_json(f"{GCP_PATH}/counter.json")['count']
    except Exception as e:
        print(f"No counter file found: {str(e)} \nCreating counter file and initializing to 1")

        try:
            blob = BUCKET.blob(f"{GCP_PATH}/counter.json")
            blob.upload_from_string(
                data=json.dumps({'count': 1}),
                content_type='application/json'
            )
            COUNTER = 0
        except Exception as e: 
            print(f"Failed creating counter.json. Exiting with exception: {str(e)}")
            sys.exit()

    FINAL_COUNTER = COUNTER + MAX_EXPORTS_TO_LOAD - 1 # Set final counter value 
    print(f"Current export counter: {COUNTER}")
    print(f"Final export counter: {FINAL_COUNTER}")
    
    # 2 - Verify dataset is present, if not create
    try:
        BIGQUERY_CLIENT.get_dataset(GCP_DATASET)
    except Exception as e:
        print(f"Dataset {GCP_PROJECT}.{GCP_DATASET} not found: {str(e)} \nCreating empty dataset.")

        try:
            DATASET = BIGQUERY_CLIENT.create_dataset(bigquery.Dataset(f"{GCP_PROJECT}.{GCP_DATASET}"), timeout=30) 
        except Exception as e:
            print(f"Failed creating dataset {GCP_PROJECT}.{GCP_DATASET}. Exiting with exception: {str(e)}")
            sys.exit()

    # 3 - Load manifest and store as global for parsing in load functions
    try:
        MANIFEST = read_json(f"{GCP_PATH}/exportmanifest.json")
        EXPORT = next((export for export in MANIFEST['exports'] if export['counter'] == COUNTER), None)
        ROOT_URL = EXPORT['rootUrl']
        print(f"Current root url: {ROOT_URL}")
    except Exception as e:
        print(f"Failed to find next export from manifest {GCP_PATH}/exportmanifest.json. Exiting with exception: {str(e)}")
        if (EXPORT == None):
            print(f"No export found with counter value of {COUNTER}")
        sys.exit()
    return

# Load each array of defintion avro files
def load_definitions():    
    definition_types = ['pageDefinitionsFile', 'featureDefinitionsFile', 'trackTypeDefinitionsFile', 'guideDefinitionsFile'] # Array of definition file types, each containing an array of avro files to be loaded
    definition_table_names = ['allpages', 'allfeatures', 'alltracktypes', 'allguides'] # Array of table names corresponding to definition files

    # For each type of definition file, if present, load all avro files defined in array
    for i,definition_type in enumerate(definition_types):
        if (definition_type in EXPORT):
            print(f"\tLoading definitions in: {definition_type}")
            load_definitions_tables(EXPORT[definition_type], definition_table_names[i])
    return

# Load each array of event files (allEvents + matchedEvents)
def load_events():
    for time_period in EXPORT['timeDependent']:
        period_id = time_period['periodId'].split('T')[0].replace('-', '') # Format period id in same format as BQ partition
        print(f"\tLoading event files for period {period_id}")

        # Load all events file, if present
        if ('allEvents' in time_period):
            load_events_tables(time_period['allEvents']['files'], period_id, 'allevents')
        
        # Load matched events files, if present
        if ('matchedEvents' in time_period):
            for i,matched_event in enumerate(time_period['matchedEvents']):
                event_id = matched_event['id'].split('/')[1]
                print(f"\t\tLoading event files for matched event {event_id}")
                load_events_tables(matched_event['files'], period_id, event_id)
    return

# After loading is completed perform any necessary cleanup
# 1. Iterate and save counter file
# Optionally you may want to delete loaded exports here
def cleanup():
    print(f"Performing final cleanup before exiting.")

    # 1. Iterate and save counter file
    try:
        print(f"\tUpdating counter.json. New value for counter: {COUNTER + 1}")
        blob = BUCKET.blob(f"{GCP_PATH}/counter.json")
        blob.upload_from_string(
            data=json.dumps({'count': COUNTER + 1}),
            content_type='application/json'
        )
    except Exception as e: 
        print(f"\tFailed updating counter.json. Exiting with exception: {str(e)}")
        sys.exit()
    return

# Perform one time setup, including reading in exportmanifest.json and counter.json
setup()

# Load exports until FINAL_COUNTER is reached, or out of exports to read
while(COUNTER <= FINAL_COUNTER):
    # Kick off async jobs to load all definition and event files from GCS to BQ
    load_definitions()
    load_events()

    # Wait here for all async jobs  to complete
    while(JOBS_FINISHED < JOBS_STARTED):
        print(f"{JOBS_STARTED} jobs started, {JOBS_FINISHED} jobs finished.")
        time.sleep(5)

    print(f"All jobs finished for export with {COUNTER}. Moving on to next export.")

    # Iterate counter and associated globals 
    try:
        COUNTER = COUNTER + 1
        EXPORT = next((export for export in MANIFEST['exports'] if export['counter'] == COUNTER), None)
        ROOT_URL = EXPORT['rootUrl']
    except Exception as e:
        print(f"Failed to find next export from manifest {GCP_PATH}/exportmanifest.json. Exiting with exception: {str(e)}")
        if (EXPORT == None):
            print(f"No export found with counter value of {COUNTER}")
            break

print(f"Done loading export. Last export loaded was export {COUNTER - 1}. Moving on to cleanup.")

# cleanup() # Disabled by default to prevent iterating counter during testing