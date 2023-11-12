# Automate GL journal entry CSV file creation

Purpose of this application is to automate GL journal entry CSV file creation from manual journal entries and bank statement.
Role of dbt is to create a CSV file which is then imported to GL system. This CSV file has date, account number, an optional dimension, amount and memo fields.

```csv
pvm,tili,dimensio,summa,muistio
2022-01-05,2871,hbsp,-62.0,hbsp
2022-01-05,2939,,12.0,hbsp
2022-01-05,7230,,50.0,hbsp
...
```

In Snowflake the resulting GL journal lines can be queried from table `v2023.gl_csv` _(notice that this in Snowflake numeric values and memo contents are have been replaced with mock [faked](https://pypi.org/project/Faker/) values)_.

```
ollipoyry#COMPUTE_WH@OLLIDEMODB.PUBLIC>select * from v2023.gl_csv where pvm >= '2023-01-01' order by pvm, tili limit 5;
+------------+------+----------+------------------------+----------------------------------------------------+
| PVM        | TILI | DIMENSIO | SUMMA                  | MUISTIO                                            |
|------------+------+----------+------------------------+----------------------------------------------------|
| 2023-01-01 | 2940 | NULL     | 886.43                 | "Fundamental stable core"                          |
| 2023-01-01 | 7700 |          | 9905.18                | "Gustafsson"                                       |
| 2023-01-12 | 1910 | NULL     | -2.600000000000000e+00 | "RF57200126320778 Verohallinto FI5689199710000724" |
| 2023-01-12 | 9900 | NULL     | 2.600000000000000e+00  | "RF57200126320778 Verohallinto FI5689199710000724" |
| 2023-01-23 | 1910 | NULL     | -3.434100000000000e+02 | "RF57200126320778 Verohallinto FI5689199710000724" |
+------------+------+----------+------------------------+----------------------------------------------------+
5 Row(s) produced. Time Elapsed: 0.135s
ollipoyry#COMPUTE_WH@OLLIDEMODB.PUBLIC>
```

Manual journal entries are generated from simple entries transformed to two or more debit/credit entries with `glrules` Python package. How a statement is created is defined in .yaml file `glrules.yml`.

![gl_csv lineage](gl_csv.png)

Bank statement items are matched to GL-rules based on regex patterns stored to a seed tables `vientityyppi` and its detail table `vientityyppi_vienti`.
There might be several matches for one bank statement item. In this case the first match is selected by using first applicable row from seed file.
DuckDB used in local testing allows 'DISTINCT ON' clause also supported by PostgreSQL but which is not supported by Snowflake.
Instead of 'DISTINCT ON' clause Snowflake uses 'QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)' clause.
Also Snowflake does not support 'regexp_matches' function available in DuckDB but instead uses 'REGEXP' operator.

```sql
SELECT 
{% if target.type == 'duckdb' %}
DISTINCT ON (  a.jarjno, b.kdi ) 
{% endif %}
* 
from  {{ ref( 'raw_tiliote' ) }} a,
 {{ ref( 'seed_vientityyppi' ) }} b
WHERE  (
    ( ifnull( b.similar_vs_like, false ) = false AND a.viite LIKE b.template )
OR    ( ifnull( b.similar_vs_like, false ) = true  AND  
{% if target.type == 'snowflake' %}
 a.viite REGEXP b.template
{% else %}
regexp_matches( a.viite, b.template )
{% endif %}
))
{% if target.type == 'snowflake' %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.jarjno, b.kdi ORDER BY a.jarjno, b.kdi) = 1
{% endif %}
ORDER BY  a.jarjno, not ifnull( b.similar_vs_like, false ), b.kdi
```

## Python script to generate GL transactions from manual entries

Manual journal entries are created in a spreadsheet with a specific format.
Each entry is a row in the spreadsheet. 
Entry has `type` and a varying number of other attributes like `date` and `amount`.
All attributes are concatenated into one cell under column header `Vienti`.
The cell has a JSON type format without the curly braces like:

```json
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

## Settings for Python scripts in .env

Above Python scripts read settings from `.env` file like below:

```shell
vuosi = 2023
manual_journal.inputDir = ~/Library/CloudStorage/Dropbox/Kirjanpito2023
manual_journal.inputFile = Kirjanpitoviennit2023.xlsx
manual_journal.inputSheet = "Käteisviennit 2023"
manual_journal.outputFile = ~/oma/v2023/2023/manual_journal_entries.parquet

bank_statement.outputFile = ~/oma/v2023/2023/bank_statement.parquet

masked.manual_journal.outputFile = /Users/olli/oma/v2023/masked/manual_journal_entries.parquet
masked.bank_statement.outputFile = /Users/olli/oma/v2023/masked/bank_statement.parquet

AWS_ACCESS_KEY_ID=AKIART6NES5O37RF5WM6
AWS_SECRET_ACCESS_KEY=***
```

## Running dbt against local DuckDB

[DuckDB](https://duckdb.org/) an in-process SQL OLAP database which can be used for testing dbt models locally.

- [DuckDB setup | dbt Developer Hub](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup)
- [Transforming Data Engineering: A Deep Dive into dbt with DuckDB | by Felix Gutierrez | Oct, 2023 | Data Engineer Things](https://blog.det.life/transforming-data-engineering-a-deep-dive-into-dbt-with-duckdb-ddd3a0c1e0c2)
- [Use dbt and Duckdb instead of Spark in data pipelines | by Niels Claeys | datamindedbe | Medium](https://medium.com/datamindedbe/use-dbt-and-duckdb-instead-of-spark-in-data-pipelines-9063a31ea2b5)

This project has a `profiles.yml` file which defines a profile `local` for DuckDB, e.g. `dbt run --profile local`.

## Staging data to Snowflake

The masked parquet files are uploaded to S3 and staged to Snowflake:

![External sources](sources.png)

Stages are created manually to Snowflake with SQL like below before running dbt:

```sql
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

Running dbt on local machine against Snowflake requires setting environment variables like below:

```shell
export SNOWFLAKE_SANDBOX_ACCOUNT=habzhbl-gd56614
export SNOWFLAKE_SANDBOX_USER=ollipoyry
export SNOWFLAKE_SANDBOX_PASSWORD="***"
```

Otionally define profile with `--profile v2023_prod`.

```shell
dbt seed             
dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
dbt run
```
