import pandas as pd
import xml.etree.cElementTree as ET
import glob
import os

from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

outputFile = os.environ['bank_statement.outputFile']
homeDir = os.path.expanduser("~")
filePath = os.path.join(homeDir, "Downloads") + "/BO-205364-*-*.xml"
print('filePath', filePath)
files = glob.glob(filePath, recursive=False)
downloadedFileWithPath = max(files, key=os.path.getctime)

ns = {"doc": "urn:schemas-microsoft-com:office:spreadsheet"}

tree = ET.parse(downloadedFileWithPath)
root = tree.getroot()

def getvalueofnode(node):
    """ return node text or None """
    return node.text if node is not None else None

def Parse():
    colNames = [] # ['Kirjauspäivä', 'Arvopäivä', 'Viite/viesti', 'Kpl', 'Määrä EUR', 'Kirjaussaldo EUR', 'Tila', 'Arkistointitunnus']
    data = []
    for i, node in enumerate(root.findall('.//doc:Row', ns)):
        if i == 0:
            colNames.extend(
                getvalueofnode(cell.find('doc:Data', ns))
                for cell in node.findall('doc:Cell', ns)
            )
        else:
            row = { 'Rivino': i }
            for j, cell in enumerate(node.findall('doc:Cell', ns)):
                row[colNames[j]] = getvalueofnode(cell.find('doc:Data', ns))
            data.append(row)
    return(pd.DataFrame(data))

df = Parse()
print(df)
df.to_parquet(outputFile, compression='gzip' )