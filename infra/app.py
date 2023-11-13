#!/usr/bin/env python3

import aws_cdk as cdk

from infra.glue_pipeline_stack import GluePipelineStack
from infra.infra_stack import InfraStack

app = cdk.App()
InfraStack(app, "InfraStack")

GluePipelineStack(app, "GluePipelineStack")
cdk.Tags.of(app).add("creator", "olli-poyry")
cdk.Tags.of(app).add("owner", "ml-team")

app.synth()
