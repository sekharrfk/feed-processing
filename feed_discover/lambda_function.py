import json
import logging
import os

import boto3
import requests
from pymongo import MongoClient

from mongo_utils import write_document_to_mongo

# configuration
env = os.environ.get("ENV")
aws_region = os.environ.get("REGION")
user_id = os.environ.get("USER_ID")

# aws service clients
# initialize here once so that it can be reused across lambda warm starts
ssm_client = boto3.client(service_name="ssm", region_name=aws_region)

s3_resource = boto3.resource(service_name="s3", region_name=aws_region)
s3_client = s3_resource.meta.client


def lambda_handler(event, context):
    """
    This function, upon receiving an s3 event, triggers feed processing(orchestrator jobs) for a connector subject to the following conditions:
    1. all feed files for the connector are present
    2. all the feed files are latest i.e. haven't been processed earlier
    :param event: s3 file upload notification event
    """
    # Extract the domain id from the file path
    domain_id = fetch_domain(event)
    if domain_id is None or domain_id == "":
        return
    logging.info("Received event from domain: {domain_id}".format(domain_id=domain_id))

    # instantiate mongo connection
    connection_string = fetch_connection_string_from_parameter_store("/{env}/auth/RFK_MONGO_URL".format(env=env))
    mongo_client = MongoClient(connection_string)

    # Write the event to mongoDB for bookkeeping purposes
    query = {"responseElements.x-amz-request-id": event["Records"][0]["responseElements"]["x-amz-request-id"]}
    is_event_stored = write_document_to_mongo(mongo_client=mongo_client, domain_id=domain_id,
                                              document=event["Records"][0], query=query)
    if is_event_stored:
        logging.info("Input event is written to mongodb")
    else:
        logging.info("Input event is already present in mongodb. This might be because it's a re-run")

    # Since the connector information isn't present in the s3 event notification, go through all the connectors
    # that have the s3 file in their config and return the first matching connector
    connector = fetch_connector(domain_id, event)
    if connector is None:
        logging.info("No connector found that matches the s3 file. Skipping further processing")
        return
    connector_id = connector["connectorId"]

    # Check if all the files from the connector config are available in s3
    # If at least one of them isn't available, stop further processing
    feed_files = fetch_feed_files_from_connector_config(connector)
    missing_feed_files = get_missing_feed_files(feed_files)
    if len(missing_feed_files) > 0:
        logging.info("Following feed files are missing: {}".format(",".join(missing_feed_files)))
        logging.info("Skipping further processing")
        return

    # Check if all the feed files are latest - not processed before
    min_feed_files_timestamp = get_min_feed_files_time(feed_files)
    # TODO: This stat isn't present in orchestrator job yet. add the stat in orchestrator job
    previous_feed_run_time = fetch_previous_run_time(domain_id, connector_id)
    if previous_feed_run_time != 0 and min_feed_files_timestamp < previous_feed_run_time:
        logging.info("No new feed files")
        return

    trigger_orchestrator_job(domain_id, connector_id)


def fetch_domain(event):
    """
    This function extracts the domain id from the s3 file key.
    Note that the s3 file must be in the following path
    s3://rfk-datasync-<env>/customer_feed_aws_sftp/<domain_hash>/...
    :param event: s3 file upload notification event
    :return: domain_id
    """
    s3_object_key = event["Records"][0]["s3"]["object"]["key"]
    domain_id = s3_object_key.split("/")[1]
    return domain_id


def fetch_connection_string_from_parameter_store(name):
    """
    This function retrieves the parameter 'name' from parameter store
    :param name: parameter to be retrieved
    """
    result = ssm_client.get_parameter(Name=name, WithDecryption=True)
    return result['Parameter']['Value']


def fetch_connector(domain_id, event):
    """
    This function checks all the feed_crawler connector configs and returns the connector that has the s3 file.
    If the s3 file isn't present in any of the connector configs, then it returns None
    """
    # Get the list of connectors for a domain
    url = "https://msapi.{env}.rfksrv.com/connector-api/v1/connectors".format(env=env)
    headers = {'rfk.userId': user_id, 'rfk.domain': domain_id}
    response = api_call_helper("GET", url, None, headers, None)
    connectors = response.json()["connectors"]

    # Get the feed file information from the event
    s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    s3_object_key = event["Records"][0]["s3"]["object"]["key"]

    # Check if feed file is present in any of the connector
    for connector in connectors:
        if connector["type"] != "feed_crawler":
            continue
        connector_config_files = connector["crawler"]["feedCrawlerConfig"]["files"]
        for f in connector_config_files:
            if f["source"]["type"] != "s3":
                continue
            connector_config_bucket = f["source"]["s3"]["bucket"]
            connector_config_object_key = f["source"]["s3"]["basePath"] + f["source"]["s3"]["fileName"]
            if s3_bucket == connector_config_bucket and s3_object_key == connector_config_object_key:
                return connector
    return None


def fetch_domain_and_connector(event):
    """
    This function extracts the domain_id and connector_id from the s3 file path.
    Note that the s3 file path must be of the format "s3://<bucket>/customer_feed_aws_sftp/<domain_id>/<connector_id>/..."
    Ex: s3://rfk-datasync-staging/customer_feed_aws_sftp/188167466/453385/prod_biglots_masterProducts.txt.gz
    :param event: s3 event
    :return: (domain_id, connector_id)
    """
    s3_object_key = event["Records"][0]["s3"]["object"]["key"]
    domain_id = s3_object_key.split("/")[1]
    connector_id = s3_object_key.split("/")[2]
    # TODO: Add a check to see if the domain_id and connector_id are valid
    # This check is needed because there is a possibility that an s3 event notification can be received from other files
    # Call connector api to check if the connector is a valid connector
    return domain_id, connector_id


def fetch_feed_files_from_connector_config(connector):
    """
    This function fetches the feed files defines in the connector config
    :return: list of feed files
    """
    feed_files = []

    # extract the feed files from the connector config
    # TODO: Add a sample connector config for better understanding
    for ff in connector["crawler"]["feedCrawlerConfig"]["files"]:
        s3_bucket = ff["source"]["s3"]["bucket"]
        s3_object_key = ff["source"]["s3"]["basePath"] + ff["source"]["s3"]["fileName"]
        feed_file = {"bucket": s3_bucket, "key": s3_object_key}
        feed_files.append(feed_file)

    return feed_files


def get_min_feed_files_time(feed_files):
    """
    This function fetches the timestamp(UTC) of each feed file and returns the earliest timestamp
    :return:
    """
    min_feed_files_time = None

    for feed_file in feed_files:
        current_file_time = get_file_timestamp(feed_file["bucket"], feed_file["key"])
        if min_feed_files_time is None:
            min_feed_files_time = current_file_time
            continue

        if current_file_time < min_feed_files_time:
            min_feed_files_time = current_file_time

    return min_feed_files_time


def get_file_timestamp(bucket, key):
    """
    This function fetches the unix timestamp of the file
    """
    s3_object = s3_resource.Object(bucket, key)
    return int(s3_object.last_modified.timestamp())


def fetch_previous_run_time(domain, connector_id):
    """
    This function fetches the latest job run of the connector.
    The latest job run's feed time is retrieved from the job statistics.
    If there are no previous runs, then a value of 0 is returned.
    """

    url = "http://msapi.{env}.rfksrv.com/job-orchestrator/v1/domains/{domain_id}/jobs".format(env=env, domain_id=domain)
    query_params = {"filter": {"connectorId": connector_id}}
    headers = {'rfk.userId': user_id}
    response = api_call_helper("GET", url, query_params, headers, None)

    job_count = response.json()["count"]
    # return a value of 0 if there are no previous runs
    if job_count == 0:
        return 0

    # get the last job id statistics
    last_job_id = response.json()["jobs"][0]["jobId"]
    url = "http://msapi.{env}.rfksrv.com/job-orchestrator/v1/domains/{domain_id}/jobs/{job_id}/stats".format(env=env,
                                                                                                             domain_id=domain,
                                                                                                             job_id=last_job_id)
    headers = {'rfk.userId': user_id}
    response = api_call_helper("GET", url, None, headers, None)

    # get the last job run's feed received time
    # TODO: Should this value be 0 ?
    last_feed_run_time = 0
    last_job_stats = response.json()["stats"]
    for stat in last_job_stats:
        if stat["name"] == "feedReceivedTime":
            last_feed_run_time = stat["value"]
            break
    return last_feed_run_time


def trigger_orchestrator_job(domain_id, connector_id):
    url = "http://msapi.{env}.rfksrv.com/job-orchestrator/v1/domains/{domain}/jobs".format(env=env, domain=domain_id)
    payload = {
        "job": {
            "type": "feed_crawler",
            "source": {
                "connector": {
                    "id": connector_id
                }
            }
        }
    }
    headers = {'rfk.userId': user_id, 'Content-Type': 'application/json'}

    response = api_call_helper("POST", url, None, headers, payload)
    logging.info("orchestrator job id: {}".format(response.json()["job"]["jobId"]))


def api_call_helper(method, url, params, headers, payload):
    response = requests.request(method, url, params=json.dumps(params), headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        # Added the curl format for better debugging
        # https://stackoverflow.com/a/17936634
        command = "curl -X {method} -H {headers} -d '{data}' '{uri}'"
        method = response.request.method
        uri = response.request.url
        data = response.request.body
        headers = ['"{0}: {1}"'.format(k, v) for k, v in response.request.headers.items()]
        headers = " -H ".join(headers)
        curl_call = command.format(method=method, headers=headers, data=data, uri=uri)
        raise Exception("Error while calling api: {}".format(curl_call))
    return response


def get_missing_feed_files(feed_files):
    """
    This function checks if all the files are present in s3. If not, returns the list of missing files
    :return: list of missing files
    """
    missing_feed_files = []

    for feed_file in feed_files:
        bucket = feed_file["bucket"]
        key = feed_file["key"]
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                missing_feed_files.append(feed_file)
            else:
                raise Exception("Error while calling s3")

    return missing_feed_files
