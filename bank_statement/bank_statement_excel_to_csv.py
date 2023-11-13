import os
import xml.etree.cElementTree as ET

import boto3
import numpy as np
import pandas as pd
from faker import Faker

# This script is used to parse the bank statement Excel XML file to a parquet file.
# Excel sheet should have first row as column names.

# filePath = os.path.join(homeDir, "Downloads") + "/BO-205364-*-*.xml"

fake = Faker("fi-FI")
s3 = boto3.client("s3")
BUCKET_NAME = "essaimdev"
LOCAL_FILE = "/tmp/bank-statement.xml"
OUTPUT_FILE = "/tmp/bank-statement.parquet"

# s3://essaimdev/bank_statement/BO-205364-20231108-1358075592.xml
get_last_modified = lambda obj: int(obj["LastModified"].strftime("%s"))
objs = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="bank_statement/excel_xml/BO-")[
    "Contents"
]
last_added = [obj["Key"] for obj in sorted(objs, key=get_last_modified)][0]
print("latest bank statement " + last_added)
s3.download_file(BUCKET_NAME, last_added, LOCAL_FILE)

ns = {"doc": "urn:schemas-microsoft-com:office:spreadsheet"}

tree = ET.parse(LOCAL_FILE)
root = tree.getroot()


def getvalueofnode(node):
    """return node text or None"""
    return node.text if node is not None else None


def Parse():
    colNames = []
    data = []
    for i, node in enumerate(root.findall(".//doc:Row", ns)):
        if i == 0:
            colNames.extend(
                getvalueofnode(cell.find("doc:Data", ns))
                for cell in node.findall("doc:Cell", ns)
            )
        else:
            row = {"Rivino": i}
            for j, cell in enumerate(node.findall("doc:Cell", ns)):
                row[colNames[j]] = getvalueofnode(cell.find("doc:Data", ns))
            data.append(row)
    return pd.DataFrame(data)


def MaskData(df):
    print(df.columns)
    df["Määrä eur"] = df.apply(lambda x: fake.pydecimal(5, 2, True), axis=1)
    df["Kirjaussaldo EUR"] = df.apply(lambda x: fake.pydecimal(4, 2, True), axis=1)
    # df['Viite/viesti']=df.apply(lambda x: fake.company() if "OIVALO" in x else x,axis=1)

    # df.loc[df['Viite/viesti'].apply(lambda x: "OIVALO" in x  )].apply( lambda x: fake.company(), axis=1)
    viiteset = []
    for _ in range(5):
        viiteset.extend(
            (
                fake.company(),
                fake.name(),
                fake.catch_phrase(),
                "600109898403 OmaVero FI5689199710000724",
                "RF57200126320778 Verohallinto FI5689199710000724",
            )
        )
    df["Viite/viesti"] = df.apply(lambda x: np.random.choice(viiteset), axis=1)
    print(df)
    return df


df = Parse()
df = MaskData(df)
print(df)
df.to_csv(OUTPUT_FILE, index=False)
s3.upload_file(OUTPUT_FILE, BUCKET_NAME, "bank_statement/csv/bank_statement.csv")
