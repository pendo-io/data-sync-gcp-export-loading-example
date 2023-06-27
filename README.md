# Data Sync GCP Export Loading Example

This repository provides sample code for how to load Pendo Data Sync data from Google Cloud Storage (GCS) into Google BigQuery (BQ). For detailed instructions on this process, please see the (Data Sync Export Handling)[https://support.pendo.io/hc/en-us/articles/14617105854875-Data-Sync-Export-Handling] article in the Pendo Help Center.

This library of custom code snippets has been created by Pendo Professional Services, with the intent of enhancing the capabilities of Pendo products. Any and all snippets in this library are free and provided at no additional cost, and as such, are provided "AS IS. For the avoidance of doubt, the library does not include any indemnification, support, or warranties of any kind, whether express or implied. For the avoidance of doubt, these snippets are outside of the remit of the Pendo Support team so please do not reach out to them for assistance with the library of code. Please do not reach out to Pendo Support for help with these snippets, as custom code is outside of the remit of their team and responsibilities.

### Getting Started

#### Dependencies

In order to run this project you will need:

- python3
- pip3

#### Setting up Python venv

First, follow Google's instructions on (setting up a Python development environment)[https://cloud.google.com/python/docs/setup].

#### Configuring Pendo Data Sync

Next, follow the appropriate instructions for (setting up Data Sync with Google Cloud)[https://support.pendo.io/hc/en-us/articles/13929263615643-Set-up-Data-Sync-with-Google-Cloud]. Once configured, create an export to your GCS destination.

#### Cloning

To clone a local version of the project run:

```
git clone https://github.com/pendo-io/data-sync-gcp-export-loading-example.git
```

#### Installation

To install dependencies run:

```
cd data-sync-gcp-export-loading-example
pip install -r requirements.txt
```

#### Usage

Once your development environment is configured, you have successfully setup a Data Sync export, and the project is configured you can test the GCS to BQ loading code in this repository. There are two versions of this code for illustrative purposes. The first performs all GCP load and query commands synchronously as the export manifest is iterated over. This mode is not intended for production, but provides a clear picture of the logic flow of the program with properly ordered logs:

```
python load_async.py <GCP_SOURCE_BUCKET_NAME> <GCP_SOURCE_PATH_TO_APPLICATION> <GCP_DESTINATION_PROJECT_NAME> <GCP_DESTINATION_DATASET_NAME>
```

The second version performs all GCP load and query commands asynchronously in separate thread queues. This allows multiple jobs to be running in parallel for better performance. We recommend instrumenting a similar approach for your production ETL process:

```
python load_sync.py <GCP_SOURCE_BUCKET_NAME> <GCP_SOURCE_PATH_TO_APPLICATION> <GCP_DESTINATION_PROJECT_NAME> <GCP_DESTINATION_DATASET_NAME>
```

Additionally, there is a script to manually create/update a counter.json file used to track the current export to load:

```
python set_counter.py <GCP_SOURCE_BUCKET_NAME> <GCP_SOURCE_PATH_TO_APPLICATION> <COUNTER_VALUE>
```

### Notes

- For best performance, we recommend partitioning tables based on periodId. This allows for better performance for targeted re-writes in the case that data changes for a previously exported period. This sample code follows that guidance and shows one approach setting up table partitions.
- This code uses a counter that corresponds to the `counter` field in the `exportmanifest.json` to track which export needs to be loaded next. The current value for this counter is stored in a JSON file in your cloud storage. By default, the cleanup step that iterates this counter is commented out for testing.
- This code does not remove exports after loading the Avro files into the appropriate BQ tables. You may want to include a step in cleanup to remove these exports to keep cloud storage costs to a minimum.
