import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsglue.dynamicframe import DynamicFrameCollection
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *


# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    logger = glueContext.get_logger()
    frame_name = list(dfc.keys())[0]
    logger.info("frame_name " + frame_name)
    print("frame_name " + frame_name)

    dyf = dfc.select(frame_name)

    # import numpy as np
    from random import randint
    from faker import Faker

    fake = Faker("fi-FI")

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

    def mask(rec):
        rec["määrä eur"] = randint(-10000, 10000) / 100  # fake.pyfloat(5, 2, True)
        rec["kirjaussaldo eur"] = randint(-10000, 10000) / 100
        rec["viite/viesti"] = viiteset[randint(0, 4)]  # np.random.choice(viiteset)
        return rec

    print(dyf)
    masked_dynamicframe = dyf.map(f=mask)  # Map.apply(frame=dyf, f=mask)
    return DynamicFrameCollection({frame_name: masked_dynamicframe}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node CSV bank statement
CSVbankstatement_node1700552816425 = glueContext.create_dynamic_frame.from_catalog(
    database="bank_statement",
    table_name="csv",
    transformation_ctx="CSVbankstatement_node1700552816425",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
detected_df = entity_detector.detect(
    CSVbankstatement_node1700552816425,
    ["GERMANY_BANK_ACCOUNT", "UK_BANK_ACCOUNT"],
    "DetectedEntities",
)


def replace_cell(original_cell_value, sorted_reverse_start_end_tuples):
    if sorted_reverse_start_end_tuples:
        for entity in sorted_reverse_start_end_tuples:
            to_mask_value = original_cell_value[entity[0] : entity[1]]
            original_cell_value = original_cell_value.replace(to_mask_value, "xxx")
    return original_cell_value


def row_pii(column_name, original_cell_value, detected_entities):
    if column_name in detected_entities.keys():
        entities = detected_entities[column_name]
        start_end_tuples = map(
            lambda entity: (entity["start"], entity["end"]), entities
        )
        sorted_reverse_start_end_tuples = sorted(
            start_end_tuples, key=lambda start_end: start_end[1], reverse=True
        )
        return replace_cell(original_cell_value, sorted_reverse_start_end_tuples)
    return original_cell_value


row_pii_udf = udf(row_pii, StringType())


def recur(df, remaining_keys):
    if len(remaining_keys) == 0:
        return df
    else:
        head = remaining_keys[0]
        tail = remaining_keys[1:]
        modified_df = df.withColumn(
            head, row_pii_udf(lit(head), head, "DetectedEntities")
        )
        return recur(modified_df, tail)


keys = CSVbankstatement_node1700552816425.toDF().columns
updated_masked_df = recur(detected_df.toDF(), keys)
updated_masked_df = updated_masked_df.drop("DetectedEntities")

DetectSensitiveData_node1700553212609 = DynamicFrame.fromDF(
    updated_masked_df, glueContext, "updated_masked_df"
)

# Script generated for node Custom Transform
CustomTransform_node1700571009836 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {
            "DetectSensitiveData_node1700553212609": DetectSensitiveData_node1700553212609
        },
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1700571395304 = SelectFromCollection.apply(
    dfc=CustomTransform_node1700571009836,
    key=list(CustomTransform_node1700571009836.keys())[0],
    transformation_ctx="SelectFromCollection_node1700571395304",
)

# Script generated for node Masked data parquet
Maskeddataparquet_node1700553215503 = glueContext.write_dynamic_frame.from_catalog(
    frame=SelectFromCollection_node1700571395304,
    database="bank_statement",
    table_name="parquet",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="Maskeddataparquet_node1700553215503",
)

job.commit()
