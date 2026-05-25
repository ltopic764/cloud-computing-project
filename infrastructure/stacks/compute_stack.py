from aws_cdk import (
    Stack,
    Duration,
)
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_events as events # CDK module for EventBridge (scheduler)
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from constructs import Construct
import os

class ComputeStack(Stack):
    """
    Stack that creates Lambda functions and their schedulers
    Every lambda gets least privilege
    Has reference to storage_stack to know the buckets name
    """
    def __init__(self, scope: Construct, construct_id: str, storage_stack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Policy for Hacker News lambda
        hn_lambda_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW, # allow action on resource
            actions=[
                "s3:PutObject", # allow file input into S3 bucket
            ],
            resources=[
                f"{storage_stack.bucket.bucket_arn}/bronze/hacker_news/*"
            ],
        )

        # Lambda function for Hacker News site
        hn_lambda = lambda_.Function(
            self,
            "HackerNewsLambda",

            runtime=lambda_.Runtime.PYTHON_3_11,

            # Path to where the code is 
            code=lambda_.Code.from_asset("src/bronze/hacker_news"),

            # which file and funcion is entry point
            handler="handler.handler", # handler.py, handler function

            # Max time for lambda to do its job, after the time runs out AWS shuts lmbda off
            timeout=Duration.minutes(5),

            memory_size=128, # change if needed

            environment={
                "S3_BUCKET_NAME": storage_stack.bucket.bucket_name,
                "LOG_LEVEL": "INFO",
                "HN_MAX_ITEMS_PER_RUN": "1000",
            },
        )

        # add policy
        hn_lambda.add_to_role_policy(hn_lambda_policy)

        hn_schedule = events.Rule(
            self,
            "HackerNewsSchedule",

            schedule=events.Schedule.cron(
                minute="0",
                hour="7",
            ),

            description="Trigger Lambda every day at ",
        )

        # when this schedule is triggered call lambda
        hn_schedule.add_target(targets.LambdaFunction(hn_lambda))
