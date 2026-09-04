#!/usr/bin/env python3

import os
import aws_cdk as cdk

from stacks.storage_stack import StorageStack
from stacks.compute_stack import ComputeStack
from stacks.notifications_stack import NotificationsStack
from stacks.silver_stack import SilverStack
from stacks.gold_stack import GoldStack
from stacks.step_functions_stack import StepFunctionsStack
from stacks.ec2_stack import EC2Stack


app = cdk.App()

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION", "eu-central-1"),
)


# ============================================================
# STORAGE
# ============================================================

storage = StorageStack(
    app,
    "StorageStack",
    env=env,
)


# ============================================================
# EC2 + VPC + POSTGRESQL INFRASTRUCTURE
# ============================================================

ec2_stack = EC2Stack(
    app,
    "EC2Stack",
    env=env,
)


# ============================================================
# BRONZE / COMPUTE
# ============================================================

compute = ComputeStack(
    app,
    "ComputeStack",
    storage_stack=storage,
    env=env,
)

compute.add_dependency(storage)


# ============================================================
# SILVER
# ============================================================

silver = SilverStack(
    app,
    "SilverStack",
    storage_stack=storage,
    env=env,
)

silver.add_dependency(storage)


# ============================================================
# GOLD
# ============================================================

gold = GoldStack(
    app,
    "GoldStack",
    storage_stack=storage,
    ec2_stack=ec2_stack,
    env=env,
)

gold.add_dependency(storage)
gold.add_dependency(silver)
gold.add_dependency(ec2_stack)


# ============================================================
# NOTIFICATIONS
# ============================================================

notifications = NotificationsStack(
    app,
    "NotificationsStack",
    compute_stack=compute,
    silver_stack=silver,
    gold_stack=gold,
    env=env,
)

notifications.add_dependency(compute)
notifications.add_dependency(silver)
notifications.add_dependency(gold)


# ============================================================
# STEP FUNCTIONS
# ============================================================

step_functions = StepFunctionsStack(
    app,
    "StepFunctionsStack",
    silver_stack=silver,
    gold_stack=gold,
    notifications_stack=notifications,
    env=env,
)

step_functions.add_dependency(silver)
step_functions.add_dependency(gold)
step_functions.add_dependency(notifications)


app.synth()