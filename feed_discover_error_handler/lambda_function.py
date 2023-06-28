import logging
import os

import boto3

# configuration
env = os.environ.get("ENV")
aws_region = os.environ.get("REGION")
feed_discover_function_name = os.environ.get("FEED_DISCOVER_FUNCTION_NAME")

lambda_client = boto3.client(service_name="lambda", region_name=aws_region)


def lambda_handler(event, context):
    logging.info("Feed discovery for the following event failed: {}".format(event))
    logging.info("Retrying the feed discovery for the event")
    lambda_client.invoke(FunctionName=feed_discover_function_name,
                         InvocationType='Event',
                         Payload=event)
    pass
