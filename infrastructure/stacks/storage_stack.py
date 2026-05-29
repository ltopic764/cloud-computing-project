# Import base CDK classes
from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput
)
from aws_cdk import aws_s3 as s3 # CDK module for S3
from constructs import Construct

class StorageStack(Stack):
    """
    Stack that creates a S3 bucket
    Here will be all of the apps data, bronze, silver and gold layer
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket
        self.bucket = s3.Bucket(
            self,
            "DataLakeBucket",

            bucket_name="social-media-pipeline-datalake", # bucket name on AWS

            versioned=False, # no need for different file versions

            encryption=s3.BucketEncryption.S3_MANAGED, # AWS is in control of encryption keys

            block_public_access=s3.BlockPublicAccess.BLOCK_ALL, # our bucket is private and no one can access it

            event_bridge_enabled=True, # enable event bridge to trigger lambda when new file is uploaded

            removal_policy=RemovalPolicy.DESTROY,

            auto_delete_objects=True,
        )

        CfnOutput(
            self,
            "DataLakeBucketName",
            value=self.bucket.bucket_name,
            description="S3 buckets name that serves as a DataLake"
        )
