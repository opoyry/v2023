# Automate GL journal entry CSV file creation

## Manual journal entries

Manual journal entries are created in a spreadsheet with a specific format.
Each entry is a row in the spreadsheet. 
Entry has `type` and a varying number of other attributes like `date` and `amount`.
All attributes are concatenated into one cell under column header `Vienti`.
The cell has a JSON type format without the curly braces like:

```
'Type':50, 'Date':'01.01.2023', 'Netto':1.99, 'Account':'7700', 'Dim1':'', 'Memo': 'Google Drive','SisAlv':1.99
```

Python package `glrules` has logic to transform journal entry parameters into a valid journal entry.
The actual journal entry rules are entered in a .yaml file `glrules.yml` package.


Python script `manual_journal_entries.py` converts the spreadsheet to a parquet file.

## Bank statement

Bank statement is stored as an Excel XML file which is stored to user's Downloads directory.
It is converted to a parquet file with `bank_statement.py` script.

```
    Rivino Kirjauspäivä   Arvopäivä                                       Viite/viesti   Kpl Määrä EUR Kirjaussaldo EUR        Tila     Arkistointitunnus
0         1   04.01.2023  04.01.2023  142601280128991 Hgin Tiedepuiston Yrityshautom...  None       -62         17213.25  Toteutunut    814697    00332320
```

## Masking real data from bank statement and manual journal entries

Python script `mask.py` masks real data from bank statement and manual journal entries. bank_statement.parquet and manual_journal_entries.parquet are masked and saved to directory `masked`.
The two files are also uploaded to S3 using `boto3` package.

## Running DBT against local DuckDB


## Staging data to Snowflake

```
CREATE OR REPLACE STAGE stg_s3_manual_journal
  URL='s3://essaimdev/snowflake/gl_sample_data/manual_journal'
  CREDENTIALS=(AWS_KEY_ID='***' AWS_SECRET_KEY='***')
  ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID = 'aws/key')
  file_format = (type = PARQUET);

CREATE OR REPLACE STAGE stg_s3_bank_statement
  URL='s3://essaimdev/snowflake/gl_sample_data/bank_statement'
  CREDENTIALS=(AWS_KEY_ID='***' AWS_SECRET_KEY='***')
  ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID = 'aws/key')
  file_format = (type = PARQUET);   

CREATE OR REPLACE STAGE stg_s3_tiliote_manual_vienti
  URL='s3://essaimdev/snowflake/gl_sample_data/tiliote_manual_vienti'
  CREDENTIALS=(AWS_KEY_ID='***' AWS_SECRET_KEY='***')
  ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID = 'aws/key')
  file_format = (type = CSV);   
```

export SNOWFLAKE_SANDBOX_ACCOUNT=habzhbl-gd56614
export SNOWFLAKE_SANDBOX_USER=ollipoyry
export SNOWFLAKE_SANDBOX_PASSWORD="***"

dbt seed  --profile v2023_prod             
dbt run-operation stage_external_sources --vars "ext_full_refresh: true" --profile v2023_prod
dbt run --profile v2023_prod       