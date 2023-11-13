import datetime
import io
import json
import logging
import os
import urllib.parse

import boto3
from invoice2data.extract.invoice_template import InvoiceTemplate
from invoice2data.extract.loader import read_templates
from invoice2data.main import extract_data
from yaml import CSafeLoader as SafeLoader
from yaml import load

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")
logger.info("Loading function")

# s3://essaimdev/invoice2data/invoice_templates/
TEMPLATE_BUCKET = "essaimdev"
OUTPUT_BUCKET = "essaimdev-data"
TEMPLATE_FOLDER = "invoice2data/invoice_templates/"


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event, indent=2)}")
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    filename = key.split("/")[-1]
    tmpfilename = f"/tmp/{filename}"
    s3.download_file(bucket, key, tmpfilename)
    templates = read_templates()
    logger.info(f"Templates {templates}")
    res = extract_data(tmpfilename, templates)
    os.remove(tmpfilename)
    if res:
        json_data = json.dumps(res, default=serialize_datetime)
        key = key.replace("/invoice_pdf", "/invoice_json").replace(".pdf", ".json")
        logger.info(f"Upload to {key} {json_data}")
        s3_upload = s3.put_object(Bucket=OUTPUT_BUCKET, Key=key, Body=json_data)
        return json_data
    else:
        print(f"Parsing invoice {filename} failed")
        return f"Error in {filename}"


# Define a custom function to serialize datetime objects
def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime("%Y-%m-%d")
    raise TypeError("Type not serializable")


def prepare_template(tpl):
    # Test if all required fields are in template
    if "keywords" not in tpl.keys():
        logger.warning(
            "Failed to load template %s Missing mandatory 'keywords' field.",
            tpl["template_name"],
        )
        return None

    # Convert keywords to list, if only one
    if not isinstance(tpl["keywords"], list):
        tpl["keywords"] = [tpl["keywords"]]

    # Set excluded_keywords as empty list, if not provided
    if "exclude_keywords" not in tpl.keys():
        tpl["exclude_keywords"] = []

    # Convert excluded_keywords to list, if only one
    if not isinstance(tpl["exclude_keywords"], list):
        tpl["exclude_keywords"] = [tpl["exclude_keywords"]]

    if "priority" not in tpl.keys():
        tpl["priority"] = 5
    return tpl


def read_templates(bucket_name=TEMPLATE_BUCKET, folder=TEMPLATE_FOLDER):
    """
    Load yaml templates from template folder. Return list of dicts.

    :param folder: Folder to load templates from. If None, use default folder.
    :return: List of dicts.
    """

    output = []

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
    files = response.get("Contents")
    for file in files:
        logger.debug(f"file_name: {file['Key']}, size: {file['Size']}")
        name = file["Key"]
        if name.endswith(".yml") or name.endswith("yaml"):
            logger.info(f"Read invoice template {name}")
            outfile = io.BytesIO()
            s3.download_fileobj(bucket_name, name, outfile)
            outfile.seek(0)
            tpl = load(outfile, Loader=SafeLoader)
            tpl["template_name"] = name
            if tpl := prepare_template(tpl):
                output.append(InvoiceTemplate(tpl))
    return output


def parse_file(full_path, templates):
    res = extract_data(full_path, templates)


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)
    s3 = boto3.client("s3")
    templates = read_templates()
    print(templates)
    # Read event from file
    with open("test/test-s3-lambda-event.json") as f:
        event = json.load(f)
    lambda_handler(event, None)
    print("Done")
