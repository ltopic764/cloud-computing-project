from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_ec2 as ec2,
)
from constructs import Construct


class GoldStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        storage_stack,
        ec2_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ============================================================
        # AWS MANAGED PANDAS LAYER
        # awswrangler + pandas + pyarrow
        # ============================================================

        pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "AWSSDKPandasLayerGold",
            layer_version_arn=(
                "arn:aws:lambda:eu-central-1:336392948345:"
                "layer:AWSSDKPandas-Python311:23"
            ),
        )

        # ============================================================
        # HACKER NEWS GOLD LAMBDA
        # ============================================================

        hn_gold_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:DeleteObject",
            ],
            resources=[
                f"{storage_stack.bucket.bucket_arn}/silver/posts/*",
                f"{storage_stack.bucket.bucket_arn}/silver/users/*",
                f"{storage_stack.bucket.bucket_arn}/gold/hacker_news/*",
                storage_stack.bucket.bucket_arn,
            ],
        )

        self.hn_gold_lambda = lambda_.Function(
            self,
            "HNGoldLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            code=lambda_.Code.from_asset("../src/gold/hacker_news"),
            handler="handler.handler",
            timeout=Duration.minutes(5),
            memory_size=512,
            layers=[pandas_layer],
            environment={
                "S3_BUCKET_NAME": storage_stack.bucket.bucket_name,
                "LOG_LEVEL": "INFO",
            },
        )

        self.hn_gold_lambda.add_to_role_policy(hn_gold_policy)

        # ============================================================
        # TWITTER GOLD LAMBDA
        # ============================================================

        twitter_gold_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:DeleteObject",
            ],
            resources=[
                f"{storage_stack.bucket.bucket_arn}/silver/posts/*",
                f"{storage_stack.bucket.bucket_arn}/silver/users/*",
                f"{storage_stack.bucket.bucket_arn}/gold/twitter/*",
                storage_stack.bucket.bucket_arn,
            ],
        )

        self.twitter_gold_lambda = lambda_.Function(
            self,
            "TwitterGoldLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            code=lambda_.Code.from_asset("../src/gold/twitter"),
            handler="handler.handler",
            timeout=Duration.minutes(5),
            memory_size=512,
            layers=[pandas_layer],
            environment={
                "S3_BUCKET_NAME": storage_stack.bucket.bucket_name,
                "LOG_LEVEL": "INFO",
            },
        )

        self.twitter_gold_lambda.add_to_role_policy(
            twitter_gold_policy
        )

        # ============================================================
        # DB LOADER LAMBDA
        # Gold Parquet -> PostgreSQL
        # ============================================================

        db_loader_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:ListBucket",
            ],
            resources=[
                f"{storage_stack.bucket.bucket_arn}/gold/*",
                storage_stack.bucket.bucket_arn,
            ],
        )

        self.db_loader_lambda = lambda_.Function(
            self,
            "DBLoaderLambda",

            runtime=lambda_.Runtime.PYTHON_3_11,

            code=lambda_.Code.from_asset(
                "../src/gold/db_loader"
            ),

            handler="handler.handler",

            timeout=Duration.minutes(10),

            memory_size=512,

            layers=[pandas_layer],

            # Lambda mora da bude u VPC-u da dođe do EC2.
            vpc=ec2_stack.vpc,

            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),

            security_groups=[
                ec2_stack.lambda_security_group
            ],

            environment={
                "S3_BUCKET_NAME":
                    storage_stack.bucket.bucket_name,

                "LOG_LEVEL": "INFO",

                "POSTGRES_HOST":
                    ec2_stack.instance.instance_private_ip,

                "POSTGRES_PORT": "5432",

                "POSTGRES_DB": "social_media",

                "POSTGRES_USER": "pipeline_user",

                "POSTGRES_PASSWORD_PARAMETER":
                    "/pipeline/postgres-password",
            },
        )

        self.db_loader_lambda.add_to_role_policy(
            db_loader_policy
        )

        # Dozvola da Lambda pročita password
        # iz SSM Parameter Store-a.
        self.db_loader_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,

                actions=[
                    "ssm:GetParameter"
                ],

                resources=[
                    (
                        f"arn:aws:ssm:"
                        f"{self.region}:"
                        f"{self.account}:"
                        f"parameter/pipeline/*"
                    )
                ],
            )
        )