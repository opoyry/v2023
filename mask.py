import numpy as np
import pandas as pd
from faker import Faker
import os

fake = Faker("fi-FI")

from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

def MaskBankStatement():
    inputFile = os.environ['bank_statement.outputFile']
    outputFile = os.environ['masked.bank_statement.outputFile']
    df = pd.read_parquet(inputFile)
    print(df.columns)
    df['Määrä eur']=df.apply(lambda x: fake.pydecimal(5, 2, True),axis=1)
    df['Kirjaussaldo EUR']=df.apply(lambda x: fake.pydecimal(4, 2, True),axis=1)
    #df['Viite/viesti']=df.apply(lambda x: fake.company() if "OIVALO" in x else x,axis=1)

    # df.loc[df['Viite/viesti'].apply(lambda x: "OIVALO" in x  )].apply( lambda x: fake.company(), axis=1)
    viiteset = []
    for _ in range(5):
        viiteset.extend(
            (
                fake.company(),
                fake.name(),
                fake.catch_phrase(),
                '600109898403 OmaVero FI5689199710000724',
                'RF57200126320778 Verohallinto FI5689199710000724',
            )
        )
    df['Viite/viesti']=df.apply(lambda x: np.random.choice(viiteset),axis=1)
    print(df)
    df.to_parquet(outputFile, compression=None)

def MaskManualJournalEntries():
    inputFile = os.environ['manual_journal.outputFile']
    outputFile = os.environ['masked.manual_journal.outputFile']
    df = pd.read_parquet(inputFile)
    print(df.columns)
    df['Amount']=df.apply(lambda x: fake.pydecimal(4, 2, True),axis=1)
    viiteset = []
    for _ in range(5):
        viiteset.extend(
            (
                fake.company(),
                fake.name(),
                fake.catch_phrase(),
                '600109898403 OmaVero FI5689199710000724',
                'RF57200126320778 Verohallinto FI5689199710000724',
            )
        )
    df['Memo']=df.apply(lambda x: np.random.choice(viiteset),axis=1)
    print(df)
    df.to_parquet(outputFile, compression=None)

def UploadToS3():
    import boto3
    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )
    s3 = session.resource('s3')
    BUCKET = "essaimdev"
    outputFile = os.environ['masked.manual_journal.outputFile']
    s3.Bucket(BUCKET).upload_file(outputFile, "snowflake/gl_sample_data/manual_journal/manual_journal.parquet") # s3://essaimdev/snowflake/gl_sample_data/manual_journal/
    outputFile = os.environ['masked.bank_statement.outputFile']
    s3.Bucket(BUCKET).upload_file(outputFile, "snowflake/gl_sample_data/bank_statement/bank_statement.parquet") # s3://essaimdev/snowflake/gl_sample_data/manual_journal/

if __name__ == "__main__":
    MaskBankStatement()
    MaskManualJournalEntries()
    UploadToS3()