#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.infrastructure_stack import InfrastructureStack
from stacks.storage_stack import StorageStack
from stacks.compute_stack import ComputeStack
from stacks.notifications_stack import NotificationsStack
from stacks.silver_stack import SilverStack
from stacks.step_functions_stack import StepFunctionsStack

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
compute.add_dependency(storage)

silver = SilverStack(
    app,
    "SilverStack",
    storage_stack=storage,
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION", "eu-central-1")),
)

silver.add_dependency(storage)

notifications = NotificationsStack(
    app,
    "NotificationsStack",
    compute_stack=compute,
    silver_stack=silver,
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION", "eu-central-1"),
    ),
)
# Always deploy storage before compute
notifications.add_dependency(compute)
notifications.add_dependency(silver)

step_functions = StepFunctionsStack(
    app, 
    "StepFunctionsStack",
    silver_stack=silver,
    notifications_stack=notifications,
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION", "eu-central-1"),
    ),
)
step_functions.add_dependency(silver)
step_functions.add_dependency(notifications)

app.synth()
