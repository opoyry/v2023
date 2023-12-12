from datetime import datetime
import glob
import os

import pandas as pd
import xml.etree.cElementTree as ET


def model(dbt, session):
    dbt.config(materialized="table")

    def chooseLatestFile():
        file_name_pattern = dbt.config.get("file_name_pattern")
        print("file_name_pattern", file_name_pattern)
        files = glob.glob(file_name_pattern, recursive=False)
        return max(files, key=os.path.getctime)

    ns = {"doc": "urn:schemas-microsoft-com:office:spreadsheet"}
    tree = ET.parse(chooseLatestFile())
    root = tree.getroot()

    def getvalueofnode(node):
        """return node text or None"""
        return node.text if node is not None else None

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
    df = pd.DataFrame(data)

    df["date"] = pd.to_datetime(df["Kirjauspäivä"], format="%d.%m.%Y").dt.date

    def complex_function(x):
        return None if x is None else datetime.strptime(x, "%d.%m.%Y").dt.date

    print(df)
    # df["date2"] = df["Arvopäivä"].apply(complex_function)
    df["date2"] = df["Arvopäivä"].apply(
        lambda x: datetime.strptime(x, "%d.%m.%Y").date() if x is not None else None
    )
    df["amount"] = pd.to_numeric(df["Määrä EUR"])
    df["ref"] = df["Viite/viesti"].astype(str)
    df["ref"] = df["ref"].apply(lambda x: x.replace("\n", " "))
    df["status"] = df["Tila"].astype(str)
    df["rownum"] = df["Rivino"]

    print("bank statement df", df)
    df.info()
    df = df[["date", "date2", "amount", "ref", "rownum", "status"]]
    print("bank statement df", df)
    # df.to_parquet(outputFile, compression="gzip")
    return df
