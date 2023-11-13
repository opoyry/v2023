# Automate GL journal entry CSV file creation

Purpose of this application is to automate GL journal entry CSV file creation from manual journal entries and bank statement.
Role of dbt is to create a CSV file which is then imported to GL system. This CSV file has date, account number, an optional dimension, amount and memo fields.

Desired end result is a single CSV file ready to be uploaded to a GL application..

```csv
pvm,tili,dimensio,summa,muistio
2022-01-05,2871,hbsp,-62.0,hbsp
2022-01-05,2939,,12.0,hbsp
2022-01-05,7230,,50.0,hbsp
...
```

In Snowflake the resulting GL journal lines can be queried from table `v2023.gl_csv`.

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

Raw inputs for DBT are prepared through three pipelines:

```mermaid
graph TD;
    A[Bank]-->B["Excel (xml)"]
    B-->C[Glue Python shell job]
    C-->D["Glue table (csv)"]
    D-->E["Glue Spark job (mask)"]
    E-->F["Glue table (parquet)"]

    BA["Invoice (PDF)"]-->BB["Glue Python container job"]
    BB-->BC[Invoice data in json]
    BC-->BD["S3"]

    CA["Manual entries (Excel)"]-->CC["Python script"]
    CC-->CD["S3"]
```

> [!NOTE]
> In Snowflake bank statement data is replaced with mock [faked](https://pypi.org/project/Faker/) values in the AWS Glue job.

## AWS architecture

A lambda function using Python library [invoice2data](https://github.com/invoice-x/invoice2data) parses invoice PDF files and stores the result as JSON back to S3.
The lambda function is deployed as a [lambda container image](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html)
because `invoice2data` has native dependency for reading PDF files. [Dockerfile](read_pdf_invoices/invoice_read_lambda/Dockerfile) installs this through `RUN yum install -y poppler-utils`.

There are three types of jobs in AWS Glue: Spark, Streaming ETL, and Python shell[^1].
This project uses a Python shell job to run parse bank statement stored as Excel XML to S3 and write it back as CSV in S3.
A Spark job is used to preprocess (mask) bank statement data and write it back as parquet to S3.

[^1]: https://docs.aws.amazon.com/glue/latest/dg/add-job.html



## Manual journal entries

Manual journal entries are generated from simple entries transformed to two or more debit/credit entries with [glrules](glrules/glrule_logic.py) Python package. How a statement is created is defined in .yaml file [glrules.yml](glrules/glrules.yml).

![gl_csv lineage](doc/images/gl_csv.png)

Bank statement items are matched to GL-rules based on regex patterns stored to a seed tables [vientityyppi](dbt/seeds/vientityyppi.csv) and its detail table [vientityyppi_vienti](dbt/seeds/vientityyppi_vienti.csv).
There might be several matches for one bank statement item. In this case the first match is selected by using first applicable row from seed file.
DuckDB used in local testing allows 'DISTINCT ON' clause also supported by PostgreSQL but which is not supported by Snowflake.
There is a standard SQL equivalence using ROW_NUMBER filtering: 'QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)'.
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

## Read invoice data from PDF files 

Python package [invoice2data](https://github.com/invoice-x/invoice2data/tree/master) is used to read invoice data from PDF files.
In AWS this is done in a lambda function [lambda-read-invoice.py](read_pdf_invoices/invoice_read_lambda/lambda-read-invoice.py) which is deployed as a container image.

Resulting JSON file is uploaded to DuckDB / Snowflake and used to create GL journal entries.
[stg_invoice_vienti.sql](dbt/models/staging/stg_invoice_vienti.sql) queries JSON data stored to the database. Invoice records are joined to GL rules based on issuer and product.

```sql
insert into invoice_entry_rule ( issuer, product, account_number, dim1 ) values
( 'DNA Oyj', '0451275927 / OLLI PÖYRY', '8500', 'dna' ),
( 'Elisa Oyj','["Elisa Viihde","Saunalahti Kotikuitu"]', '8500', 'elisa' );
```

## Python script to generate GL transactions from manual entries

Manual journal entries are created in a spreadsheet with a specific format.
Each entry is a row in the spreadsheet. 
Entry has `type` and a varying number of other attributes like `date` and `amount`.
All attributes are concatenated into one cell under column header `Vienti`.
The cell has a JSON type format without the curly braces like:

```json
{ "Type":50, "Date":"01.01.2023", "Netto":1.99, "Account":"7700", "Dim1":"", "Memo": "Google Drive","SisAlv":1.99 }
```

Python package [glrules](glrules/glrule_logic.py) has logic to transform journal entry parameters into a valid journal entry.
The actual journal entry rules are entered in a .yaml file [glrules.yml](glrules/glrules.yml).


Python script [manual_journal_entries.py](manual_journal_entry/manual_journal_entries.py) converts the spreadsheet to a parquet file.

## Masking real data from bank statement and manual journal entries

When preparing incoming data locally and not in AWS Python script [mask.py](mask.py) masks real data from bank statement and manual journal entries. bank_statement.parquet and manual_journal_entries.parquet are masked and saved to directory `masked`.
The two files are also uploaded to S3 using `boto3` package.

In AWS Glue Spark job [bank_statement_csv_to_parquet.py](bank_statement/bank_statement_csv_to_parquet.py) uses a Custom Transform step to do similar masking of real data. 

![AWS Glue visual job](doc/images/Glue_bank_statement_csv_to_parquet.png)

## Staging data to Snowflake

The masked parquet files are uploaded to S3 and staged to Snowflake:

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

## Bank statement

Bank statement is stored as an Excel XML file which is stored to user's Downloads directory.
It is converted to a parquet file with [bank_statement.py](bank_statement.py) script.

```
    Rivino Kirjauspäivä   Arvopäivä                                       Viite/viesti   Kpl Määrä EUR Kirjaussaldo EUR        Tila     Arkistointitunnus
0         1   04.01.2023  04.01.2023  142601280128991 Hgin Tiedepuiston Yrityshautom...  None       -62         17213.25  Toteutunut    814697    00332320
```

![External sources](doc/images/sources.png)

Bank statement rows for which no match was found are stored to table `stg_tiliote_not_matched`. These should be resolved either
by adding a corresponding entry to source data `tiliote_manual_vienti` or a rule added to `seed_vientityyppi` with actual GL entries in `seed_vientityyppi_vienti`.

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

Running dbt on local machine against Snowflake requires setting environment variables like below:

```shell
export SNOWFLAKE_SANDBOX_ACCOUNT=habzhbl-gd56614
export SNOWFLAKE_SANDBOX_USER=ollipoyry
export SNOWFLAKE_SANDBOX_PASSWORD="***"
```

Populating Snowflake tables from seed data and Parquet files uploaded to S3 is done with below steps. 
Optionally define profile with `--profile v2023_prod`.

```shell
dbt seed             
dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
dbt run
```
