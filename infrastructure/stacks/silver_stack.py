from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
)
from constructs import Construct


class SilverStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        storage_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "AWSSDKPandasLayer",
            layer_version_arn="arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:23",
        )

        # IAM policy for HN Silver Lambda
        hn_silver_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:DeleteObject",
            ],
            resources=[
                f"{storage_stack.bucket.bucket_arn}/bronze/hacker_news/*",
                f"{storage_stack.bucket.bucket_arn}/silver/posts/*",
                f"{storage_stack.bucket.bucket_arn}/silver/users/*",
                storage_stack.bucket.bucket_arn,
            ],
        )

        # HN Silver Lambda
        self.hn_silver_lambda = lambda_.Function(
            self,
            "HNSilverLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            code=lambda_.Code.from_asset("../src/silver/hacker_news"),
            handler="handler.handler",
            timeout=Duration.minutes(10),
            memory_size=512,

            layers=[pandas_layer],

            environment={
                "S3_BUCKET_NAME": storage_stack.bucket.bucket_name,
                "LOG_LEVEL": "INFO",
                "MAX_USERS_TO_FETCH": "10", # testing
            },
        )

        self.hn_silver_lambda.add_to_role_policy(hn_silver_policy)

        # IAM policy for Twitter Silver Lambda
        twitter_silver_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:DeleteObject",
            ],
            resources=[
                f"{storage_stack.bucket.bucket_arn}/bronze/twitter/*",
                f"{storage_stack.bucket.bucket_arn}/silver/posts/*",
                f"{storage_stack.bucket.bucket_arn}/silver/users/*",
                storage_stack.bucket.bucket_arn,
            ],
        )

        # Twitter Silver Lambda
        self.twitter_silver_lambda = lambda_.Function(
            self,
            "TwitterSilverLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            code=lambda_.Code.from_asset("../src/silver/twitter"),
            handler="handler.handler",
            timeout=Duration.minutes(10),
            memory_size=1024,

            layers=[pandas_layer],

            environment={
                "S3_BUCKET_NAME": storage_stack.bucket.bucket_name,
                "LOG_LEVEL": "INFO",
            },
        )

        self.twitter_silver_lambda.add_to_role_policy(twitter_silver_policy)
