# All code in this repository is free to use, and as such, there is NO WARRANTY, SLA or SUPPORT for this code. 
# This code is meant to be a starting point for your team to help you get to value with Pendo Data Sync more quickly. 
# Please do not reach out to Pendo Support for help with these code, as Data Sync ETL is outside of the remit of their team and responsibilities.

import sys
import json
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
VALIDATE_LOAD = True # If true validates load by retrieving table and printing current table size
MAX_THREADS = 4 # Max number of threads to spread load jobs between in async mode

print(f"Loading Pendo data from {GCP_BUCKET}{GCP_PATH_TO_EXPORT} to project {GCP_PROJECT}, dataset {GCP_DATASET}")

# Read specified JSON file from cloud storage
def read_json(blob_name):
    blob = BUCKET.blob(blob_name)
    with blob.open("r") as f:
        return json.loads(blob.download_as_string(client=None))

# Load definition table based on supplied configuration
def load_definitions_table(uri, table_id, write_disposition):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO, # Specify Avro file format 
        write_disposition=write_disposition, # Write disposition (truncate/append)
        use_avro_logical_types=True # Convert Avro logical types to BQ types (e.g. TIMESTAMP) rather than raw types (e.g. INTEGER)
    )

    num_tries = 1
    load_successful = False
    while (num_tries <= MAX_NUM_TRIES):
        try:
            print(f"\t\t\tLoading table {table_id} (attempt {num_tries})")
            # Send load job
            job = BIGQUERY_CLIENT.load_table_from_uri(uri, table_id, job_config=job_config)
            result = job.result() # Waits for the job to complete.
            print(f"\t\t\tResult: {result}")

            # Validate 
            if (VALIDATE_LOAD):
                destination_table = BIGQUERY_CLIENT.get_table(table_id)
                print(f"\t\t\tTable {destination_table.num_rows} rows in length.")
            load_successful = True
            break
        except Exception as e:
            print(f"\t\t\tFailed load with exception: {e}")
        num_tries += 1

    if(not load_successful):
        print(f"Unable to load {table_id}. Exiting.")
        sys.exit()

    return

# Load event table based on supplied configuration
def load_event_table(uri, table_id, period_id):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO, # Specify Avro file format 
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Always append events
        use_avro_logical_types=True # Convert Avro logical types to BQ types (e.g. TIMESTAMP) rather than raw types (e.g. INTEGER)
    )

    num_tries = 1
    load_successful = False
    while (num_tries <= MAX_NUM_TRIES):
        try:
            # If table exists proceed with appending events to table, else load to temp table and then create table with partitioning from temp table
            try:
                # Check if table exists
                BIGQUERY_CLIENT.get_table(table_id)
                print(f"\t\t\tTable {table_id} already exists. (attempt {num_tries})")

                # Drop rows from table with period if present 
                print(f"\t\t\tDropping rows from {table_id} where periodId = {period_id} from table.")
                BIGQUERY_CLIENT.query(f"DELETE FROM `{table_id}` WHERE periodId = PARSE_DATE('%Y%m%d',  '{period_id}');")

                # Send load job
                print(f"\t\t\tAppending new rows to table {table_id}")
                job = BIGQUERY_CLIENT.load_table_from_uri(uri, table_id, job_config=job_config)
                result = job.result() # Waits for the job to complete.
                print(f"\t\t\tResult: {result}")

                # Validate 
                if (VALIDATE_LOAD):
                    destination_table = BIGQUERY_CLIENT.get_table(table_id)
                    print(f"\t\t\tTable {destination_table.num_rows} rows in length.")
                load_successful = True
            except NotFound:
                # Send load job
                print(f"\t\t\tTable {table_id} is not found. Creating temp table {table_id}_temp to load from. (attempt {num_tries})")
                job = BIGQUERY_CLIENT.load_table_from_uri(uri, f"{table_id}_temp", job_config=job_config)
                result = job.result() # Waits for the job to complete.
                print(f"\t\t\tResult: {result}")

                # Copy from temp table with partitioning
                print(f"\t\t\tCopying contents from {table_id}_temp to {table_id}")
                job = BIGQUERY_CLIENT.query(f"CREATE TABLE `{table_id}` PARTITION BY periodId AS SELECT * FROM `{table_id}_temp`;")
                result = job.result() # Waits for the job to complete.
                print(f"\t\t\tResult: {result}")

                # Remove temp table
                print(f"\t\t\tDropping {table_id}_temp")
                job = BIGQUERY_CLIENT.query(f"DROP TABLE `{table_id}_temp`")
                result = job.result() # Waits for the job to complete.
                print(f"\t\t\tResult: {result}")

                # Validate 
                if (VALIDATE_LOAD):
                    destination_table = BIGQUERY_CLIENT.get_table(table_id)
                    print(f"\t\t\tTable {destination_table.num_rows} rows in length.")
                load_successful = True
            break
        except Exception as e:
            print(f"\t\t\tFailed load with exception: {e}")
        num_tries += 1

    if(not load_successful):
        print(f"Unable to load {table_id}. Exiting.")
        sys.exit()

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
load_definitions()
load_events()
# cleanup() # Disabled by default to prevent iterating counter during testing