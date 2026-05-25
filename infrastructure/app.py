#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.infrastructure_stack import InfrastructureStack
from stacks.storage_stack import StorageStack
from stacks.compute_stack import ComputeStack

# Create CDK app
# Root object
app = cdk.App()

# Create Storage stack
storage = StorageStack(
    app,
    "StorageStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION", "eu-central-1"),
    ),
)

# Create Compute stack
compute = ComputeStack(
    app,
    "ComputeStack",
    storage_stack=storage,
    env=cdk.Environment(
         account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION", "eu-central-1"),
    ),
)

# Always deploy storage before compute
compute.add_dependency(storage)

app.synth()
