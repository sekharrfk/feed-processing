import logging

import boto3
import requests

env = "staging"
userId = '630'
aws_region = "us-east-1"
s3_resource = boto3.resource(service_name="s3", region_name=aws_region)


def lambda_handler(event, context):
    """
    This function, upon receiving an s3 event, triggers feed processing(orchestrator jobs) for a connector subject to the following conditions:
    1. all feed files for the connector are present
    2. at least one feed file is latest i.e. haven't been processed earlier
    :param event: s3 event
    """
    domain_id, connector_id = fetch_domain_and_connector(event)
    feed_files = fetch_feed_files(domain_id, connector_id)
    max_feed_files_timestamp = get_max_feed_files_time(feed_files)

    # TODO: add the stat in orchestrator job
    previous_feed_run_time = fetch_previous_run_time(domain_id, connector_id)
    if previous_feed_run_time is not None and max_feed_files_timestamp < previous_feed_run_time:
        logging.info("No new feed files")
        return

    trigger_orchestrator_job(domain_id, connector_id)


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


def fetch_feed_files(domain, connector_id):
    """
    This function fetches the feed files from the connector config
    :return: list of feed files
    """
    feed_files = []

    # fetch the connector config from the connector api
    url = "https://msapi.{env}.rfksrv.com/connector-api/v1/connectors/{connector_id}".format(env=env,
                                                                                             connector_id=connector_id)
    headers = {'rfk.userId': userId, 'rfk.domain': domain}
    response = api_call_helper(method="GET", url=url, params=None, headers=headers, payload=None)

    # extract the feed files from the connector config
    # TODO: Add a sample connector config for better understanding
    for ff in response.json()["connector"]["crawler"]["feedCrawlerConfig"]["files"]:
        s3_bucket = ff["source"]["s3"]["bucket"]
        s3_object_key = ff["source"]["s3"]["basePath"] + ff["source"]["s3"]["fileName"]
        feed_file = {"bucket": s3_bucket, "key": s3_object_key}
        feed_files.append(feed_file)

    return feed_files


def get_max_feed_files_time(feed_files):
    """
    This function fetches the timestamp(UTC) of each feed file and returns the latest timestamp
    :return:
    """
    max_feed_files_time = None

    for feed_file in feed_files:
        current_file_time = get_file_timestamp(feed_file["bucket"], feed_file["key"])
        if max_feed_files_time is None:
            max_feed_files_time = current_file_time
            continue

        if current_file_time > max_feed_files_time:
            max_feed_files_time = current_file_time

    return max_feed_files_time


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
    headers = {'rfk.userId': userId}
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
    headers = {'rfk.userId': userId}
    response = api_call_helper("GET", url, None, headers, None)

    # get the last job run's feed received time
    # TODO: Should this value be 0 ?
    last_feed_run_time = None
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
    headers = {'rfk.userId': userId, 'Content-Type': 'application/json'}

    response = api_call_helper("POST", url, None, headers, payload)
    logging.info("orchestrator job id: {}".format(response.json()["job"]["jobId"]))


def api_call_helper(method, url, params, headers, payload):
    response = requests.request(method, url, params=params, headers=headers, data=payload)
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
