from aws_cdk import (
    Stack,
    Duration,
)
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from constructs import Construct
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


class ComputeStack(Stack):
    """
    Stack that creates Lambda functions and their schedulers.
    Every lambda gets least privilege.
    Has reference to storage_stack to know the bucket name.
    """

    def __init__(self, scope: Construct, construct_id: str, storage_stack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # =========================
        # Hacker News Bronze Lambda
        # =========================

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

            code=lambda_.Code.from_asset(
                os.path.join(PROJECT_ROOT, "src", "bronze", "hacker_news")
            ),

            # which file and funcion is entry point
            handler="handler.handler", # handler.py, handler function

            timeout=Duration.minutes(5),

            memory_size=128, # change if needed

            environment={
                "S3_BUCKET_NAME": storage_stack.bucket.bucket_name,
                "LOG_LEVEL": "INFO",
                "HN_MAX_ITEMS_PER_RUN": "1000",
            },
        )

        hn_lambda.add_to_role_policy(hn_lambda_policy)

        hn_schedule = events.Rule(
            self,
            "HackerNewsSchedule",

            schedule=events.Schedule.cron(
                minute="0",
                hour="7",
            ),

            description="Trigger Lambda every day at 8",
        )

        hn_schedule.add_target(targets.LambdaFunction(hn_lambda))

        # =====================
        # Twitter Bronze Lambda
        # =====================

        # Policy for Twitter lambda
        twitter_lambda_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:PutObject",
            ],
            resources=[
                f"{storage_stack.bucket.bucket_arn}/input/twitter/*",
                f"{storage_stack.bucket.bucket_arn}/bronze/twitter/*",
            ],
        )

        twitter_lambda = lambda_.Function(
            self,
            "TwitterBronzeLambda",

            runtime=lambda_.Runtime.PYTHON_3_11,

            code=lambda_.Code.from_asset(
                os.path.join(PROJECT_ROOT, "src", "bronze", "twitter")
            ),

            handler="handler.handler",

            timeout=Duration.minutes(5),

            memory_size=128,

            environment={
                "S3_BUCKET_NAME": storage_stack.bucket.bucket_name,
                "LOG_LEVEL": "INFO",
                "TWITTER_MAX_ROWS_PER_RUN": "5000",
                "SAFETY_STOP_MS": "15000",
            },
        )

        twitter_lambda.add_to_role_policy(twitter_lambda_policy)

        # EventBridge rule for S3 Object Created events.
        # This avoids direct S3 notification dependency cycle between StorageStack and ComputeStack.
        twitter_input_rule = events.Rule(
            self,
            "TwitterInputObjectCreatedRule",

            event_pattern=events.EventPattern(
                source=[
                    "aws.s3"
                ],
                detail_type=[
                    "Object Created"
                ],
                detail={
                    "bucket": {
                        "name": [
                            storage_stack.bucket.bucket_name
                        ]
                    },
                    "object": {
                        "key": [
                            {
                                "prefix": "input/twitter/"
                            }
                        ]
                    }
                },
            ),

            description="Trigger Twitter Bronze Lambda when a file is uploaded to input/twitter/",
        )

        twitter_input_rule.add_target(
            targets.LambdaFunction(twitter_lambda)
        )