# All code in this repository is free to use, and as such, there is NO WARRANTY, SLA or SUPPORT for this code. 
# This code is meant to be a starting point for your team to help you get to value with Pendo Data Sync more quickly. 
# Please do not reach out to Pendo Support for help with these code, as Data Sync ETL is outside of the remit of their team and responsibilities.

import sys
import json
import time
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound

# Input arguments specifying which export to load and where to load it to
GCP_BUCKET = sys.argv[1] # Name of bucket containing data sync export (e.g. my-pendo-data-bucket)
GCP_PATH_TO_EXPORT = sys.argv[2] # Path to manifest of interest in bucket (e.g. datasync/<SUBSCRIPTION_ID>/<APPLICATION_ID>)
GCP_PROJECT = sys.argv[3] # Name of project to load data to in BigQuery (e.g. my-reporting-project)
GCP_DATASET = sys.argv[4] # Name of dataset to load data to in BigQuery (e.g. my-pendo-dataset)

# Google cloud connections
STORAGE_CLIENT = storage.Client()
BUCKET = STORAGE_CLIENT.bucket(GCP_BUCKET)
BIGQUERY_CLIENT = bigquery.Client()
DATASET = None

# Files/values read from cloud storage
COUNTER = None # Global counter from cloud storage indicating what export to load
MANIFEST = None # Manifest content read from cloud storage
EXPORT = None # Current export based on counter from manifest
ROOT_URL = None # Root url for export to build full path to all files loaded below

# Global config
MAX_NUM_TRIES = 3 # Maximum number of tries to load file before exiting program
MAX_THREADS = 4 # Max number of threads to spread load jobs between in async mode

# Async processing globals
THREAD_EXECUTOR = ThreadPoolExecutor(MAX_THREADS) # Executor to run async tasks in allocated threads
JOBS_STARTED = 0 # Number of jobs started, to track when all async jobs have finished
JOBS_FINISHED = 0 # Number of jobs finished, to track when all async jobs have finished

print(f"Loading Pendo data from {GCP_BUCKET}{GCP_PATH_TO_EXPORT} to project {GCP_PROJECT}, dataset {GCP_DATASET}")

# Read specified JSON file from cloud storage
def read_json(blob_name):
    blob = BUCKET.blob(blob_name)
    with blob.open("r") as f:
        return json.loads(blob.download_as_string(client=None))
    
# Submit job to thread executor upon creation and add one to number of jobs started
def start_job(job_type, job_params, attempt_number, next):
    global JOBS_STARTED, THREAD_EXECUTOR
    JOBS_STARTED += 1
    future = THREAD_EXECUTOR.submit(async_job, job_type, job_params, attempt_number, next)
    future.add_done_callback(job_finished)

# Validate result of job upon finish and add one to number of jobs finished
def job_finished(future):
    global JOBS_FINISHED
    JOBS_FINISHED += 1
    job = future.result()
    validate_async(job)


# Function to run async GCP jobs required for loading tables
def async_job(job_type, job_params, attempt_number, next):
    if (job_type == "load"):
        print(f"Running load from {job_params['uri']} to {job_params['table_id']}")
        job = BIGQUERY_CLIENT.load_table_from_uri(job_params["uri"], job_params["table_id"], job_config=job_params["job_config"])
    elif (job_type == "query"):
        print(f"Started query {job_params['query']}")
        job = BIGQUERY_CLIENT.query(job_params["query"])

    return { 
        "result": job.result(), 
        "job": job, 
        "job_type": job_type,
        "job_params": job_params,
        "attempt_number": attempt_number, 
        "next": next
    }

# Validate that async job has finished and has no next steps
# Return True if job is done with no next steps
def validate_async(async_job):
    if (async_job["job_type"] == "load"):
        print(f"Finished load from {async_job['job_params']['uri']} to {async_job['job_params']['table_id']}")
    elif (async_job["job_type"] == "query"):
        print(f"Finished query {async_job['job_params']['query']}")


    # If errors and attempts left, retry.
    # Else if errors and no attempts left, exit.
    # TODO: Find a way to simulate errors for testing.
    if (async_job["job"].errors != None):
        if (async_job["attempt_number"] <= MAX_NUM_TRIES):
            print(f"\tRetrying job {async_job['job']} with errors {async_job['job'].errors}. (Attempt {async_job['attempt_number']})")
            start_job(
                async_job["job_type"], 
                async_job["job_params"], 
                async_job["attempt_number"] + 1, 
                async_job["next"]
            )
            return False
        else:
            print(f"\tJob {async_job['job']} failed with errors {async_job['job'].errors} after {async_job['attempt_number']} attempts. Exiting.")
            sys.exit()

    # If there are other async steps to follow, kick the next step off
    if (async_job["next"] != None):
        print(f"\tNext job: {async_job['next']}")
        start_job(
            async_job["next"]["job_type"], 
            async_job["next"]["job_params"], 
            async_job["next"]["attempt_number"], 
            async_job["next"]["next"]
        )
        return False

    print("\tNo next job in series")
    return True

# Load definition table based on supplied configuration
def load_definitions_table(uri, table_id, write_disposition):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO, # Specify Avro file format 
        write_disposition=write_disposition, # Write disposition (truncate/append)
        use_avro_logical_types=True # Convert Avro logical types to BQ types (e.g. TIMESTAMP) rather than raw types (e.g. INTEGER)
    )
    
    print(f"\t\t\tAdding load job for table {table_id} to async queue.")
    start_job(
        "load", 
        {"uri": uri, "table_id": table_id, "job_config": job_config}, 
        1, 
        None
    )

    return

# Load event table based on supplied configuration
def load_event_table(uri, table_id, period_id):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO, # Specify Avro file format 
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Always append events
        use_avro_logical_types=True # Convert Avro logical types to BQ types (e.g. TIMESTAMP) rather than raw types (e.g. INTEGER)
    )

    try:
        # Check if table exists
        BIGQUERY_CLIENT.get_table(table_id)
        print(f"\t\t\tTable {table_id} already exists.")

        # Add load job to queue
        print(f"\t\t\tAdding load job for table {table_id} to async queue.")
        start_job(
            "query", 
            { "query": f"DELETE FROM `{table_id}` WHERE periodId = PARSE_DATE('%Y%m%d',  '{period_id}')" }, # Delete data for period from partitioned table
            1, 
            {
                "job_type": "load", 
                "job_params": {"uri": uri, "table_id": table_id, "job_config": job_config}, # Load data from Avro into partitioned table
                "attempt_number": 1, 
                "next": None
            }
        )
    except NotFound:
        # Add load job to queue
        print(f"\t\t\tAdding load job for table {table_id} to async queue.")
        start_job( 
            "load", 
            { "uri": uri, "table_id": f"{table_id}_temp", "job_config": job_config }, # Create temp table from data
            1, 
            {
                "job_type": "query", 
                "job_params": {"query": f"CREATE TABLE `{table_id}` PARTITION BY periodId AS SELECT * FROM `{table_id}_temp`;"}, # Create new table with partitioning from temp table
                "attempt_number": 1,
                "next": {
                    "job_type": "query", 
                    "job_params": {"query": f"DROP TABLE `{table_id}_temp`"}, # Remove temp table
                    "attempt_number": 1,
                    "next": None
                }
            }
        )

    return

# Perform upfront setup to ensure we are ready to load export
# 1 - Verify counter file is present, if not create
# 2 - Verify dataset is present, if not create
# 3 - Load manifest and store as global for parsing in load functions
def setup():
    global COUNTER, DATASET, MANIFEST, EXPORT, ROOT_URL # Globals defined as a part of setup

    # 1 - Verify counter file is present, if not create
    try: 
        COUNTER = read_json(f"{GCP_PATH_TO_EXPORT}/counter.json")["count"]
    except Exception as e:
        print(f"No counter file found: {str(e)} \nCreating counter file and initializing to 1")

        try:
            blob = BUCKET.blob(f"{GCP_PATH_TO_EXPORT}/counter.json")
            blob.upload_from_string(
                data=json.dumps({"count": 1}),
                content_type='application/json'
            )
            COUNTER = 1
        except Exception as e: 
            print(f"Failed creating counter.json. Exiting with exception: {str(e)}")
            sys.exit()
    print(f"Current export counter: {COUNTER}")
    
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
        MANIFEST = read_json(f"{GCP_PATH_TO_EXPORT}/exportmanifest.json")
        EXPORT = next((export for export in MANIFEST["exports"] if export["counter"] == COUNTER), None)
        ROOT_URL = EXPORT["rootUrl"]
        print(f"Current root url: {ROOT_URL}")
    except Exception as e:
        print(f"Failed next export from manifest {GCP_PATH_TO_EXPORT}/exportmanifest.json. Exiting with exception: {str(e)}")
        if (EXPORT == None):
            print(f"No export found with counter value of {COUNTER}")
        sys.exit()

# Load each array of defintion avro files
def load_definitions():    
    definition_types = ["pageDefinitionsFile", "featureDefinitionsFile", "trackTypeDefinitionsFile", "guideDefinitionsFile"] # Array of definition file types, each containing an array of avro files to be loaded
    definition_table_names = ["allpages", "allfeatures", "alltracktypes", "allguides"] # Array of table names corresponding to definition files

    # For each type of definition file, load all avro files defined in array
    for i,definition_type in enumerate(definition_types):
        print(f"\tLoading definitions in: {definition_type}")

        for j,definition_file in enumerate(EXPORT[definition_type]):
            print(f"\t\tLoading definition: {definition_file}")

            load_definitions_table(
                f"{ROOT_URL}/{definition_file}", # Source file URI
                f"{GCP_PROJECT}.{GCP_DATASET}.{definition_table_names[i]}", # Destination table name
                bigquery.WriteDisposition.WRITE_TRUNCATE if j == 0 else bigquery.WriteDisposition.WRITE_APPEND # Truncate for first file in array, otherwise append
            )

# Load each array of event files (allEvents + matchedEvents)
def load_events():
    for time_period in EXPORT["timeDependent"]:
        period_id = time_period['periodId'].split("T")[0].replace("-", "") # Format period id in same format as BQ partition
        print(f"\tLoading event files for period {period_id}")

        # Load all events file
        for all_events_file in time_period["allEvents"]["files"]:
            print(f"\t\tLoading all events file: {all_events_file}")
            load_event_table(
                f"{ROOT_URL}/{all_events_file}", # Source file URI
                f"{GCP_PROJECT}.{GCP_DATASET}.allevents", # Destination table name,
                period_id # Period id of partition to load data to
            )
        
        # Load matched events files
        for i,matched_event in enumerate(time_period["matchedEvents"]):
            print(f"\t\tLoading event files for matched event {matched_event['id']}")
            for matched_event_file in matched_event["files"]:
                print(f"\t\tLoading matched event file: {matched_event_file}")
                load_event_table(
                    f"{ROOT_URL}/{matched_event_file}", # Source file URI
                    f"{GCP_PROJECT}.{GCP_DATASET}.{matched_event['id'].split('/')[1]}", # Destination table name,
                    period_id # Period id of partition to load data to
                )

# After loading is completed perform any necessary cleanup
# 1. Iterate and save counter file
def cleanup():
    print(f"Performing final cleanup before exiting.")

    # 1. Iterate and save counter file
    try:
        print(f"\tUpdating counter.json. New value for counter: {COUNTER + 1}")
        blob = BUCKET.blob(f"{GCP_PATH_TO_EXPORT}/counter.json")
        blob.upload_from_string(
            data=json.dumps({"count": COUNTER + 1}),
            content_type='application/json'
        )
    except Exception as e: 
        print(f"\tFailed updating counter.json. Exiting with exception: {str(e)}")
        sys.exit()


setup()
# load_definitions()
load_events()

# Wait here for all async jobs  to complete
while(JOBS_FINISHED < JOBS_STARTED):
    print(f"{JOBS_STARTED} jobs started, {JOBS_FINISHED} jobs finished.")
    time.sleep(5)

print("All jobs finished. Moving on to cleanup.")

# cleanup() # Disabled by default to prevent iterating counter during testing