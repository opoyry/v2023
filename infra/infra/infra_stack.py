from aws_cdk import (
    Duration,
    Stack,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_sns as sns,
)
from aws_cdk import (
    aws_sns_subscriptions as subs,
)
from aws_cdk import (
    aws_sqs as sqs,
)
from constructs import Construct


class InfraStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        queue = sqs.Queue(
            self,
            "OlliCdkInfraQueue",
            visibility_timeout=Duration.seconds(300),
        )

        if False:
            topic = sns.Topic(self, "InfraTopic")
            topic.add_subscription(subs.SqsSubscription(queue))

        s3.Bucket(
            self,
            id="bucket123",
            bucket_name="essaim-cdk-bucket-02",  # Provide a bucket name here
        )
