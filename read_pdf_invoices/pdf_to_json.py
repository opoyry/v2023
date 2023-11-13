import codecs
import datetime
import json
import os.path

import pandas as pd
from dotenv import load_dotenv
from invoice2data.extract.loader import read_templates
from invoice2data.main import extract_data
from invoice2data.output import to_json

load_dotenv()

templateDir = os.environ["read_pdf_invoices.templateDir"]
invoicePdfDir = os.environ["read_pdf_invoices.invoicePdfDir"]
invoiceJsonDir = os.environ["read_pdf_invoices.invoiceJsonDir"]
print("templateDir", templateDir)
templates = read_templates(templateDir)
print("templates", templates)

# Iterate pdf files in directory invoiceDir
# For each file, extract data using invoice2data

data = []
for filename in os.listdir(invoicePdfDir):
    # Get the full path of the item
    full_path = os.path.join(invoicePdfDir, filename)
    if os.path.isfile(full_path) and filename.endswith(".pdf"):
        print(f"Processing file: {full_path}")
        # Here you can add the code to process the file
        res = extract_data(full_path, templates)
        if res is False:
            print("File not parsed", full_path)
            continue
        print(full_path, res)
        res.update({"filename": filename})

        if hasattr(res["amount"], "__len__"):
            print("Amount is an array, use max value", res["amount"])
            res["max_amount"] = max(res["amount"])

        if "amount_vat_24" in res and "amount_vat" not in res:
            res["amount_vat"] = res["amount_vat_24"]

        if "phone_number" in res and "product" not in res:
            res["product"] = f'{res["phone_number"]} / {res["phone_user"]}'

        data.append(res)
        # to_json.write_to_file(res, "./out" + filename.replace("pdf", "json"))
        filename = os.path.join(invoiceJsonDir, filename.replace("pdf", "json"))
        sJson = to_json.format_item(res, "%Y-%m-%d")
        with codecs.open(filename, "w", encoding="utf-8") as json_file:
            json.dump(
                res,
                json_file,
                indent=4,
                sort_keys=False,
                ensure_ascii=False,
            )
filename = os.path.join(invoiceJsonDir, "invoices.json")
to_json.write_to_file(data, filename)


# Define a custom function to serialize datetime objects
def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


df = pd.DataFrame(data)
print(df)
print(json.dumps(data, default=serialize_datetime, indent=4))
