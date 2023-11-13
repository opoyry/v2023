from diagrams import Diagram
from diagrams.aws.compute import EC2, Lambda

with Diagram("Lambda Invocation", show=False):
    parent_lambda = Lambda("Parent Lambda")
    child_lambda = Lambda("Child Lambda")

    parent_lambda >> child_lambda
