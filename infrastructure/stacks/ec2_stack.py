from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
)
from constructs import Construct


class EC2Stack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ============================================================
        # VPC
        # ============================================================

        self.vpc = ec2.Vpc(
            self,
            "PipelineVPC",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                # EC2 mora da ima pristup internetu
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),

                # Lambda ide ovde.
                # Nema direktan pristup internetu.
                ec2.SubnetConfiguration(
                    name="Isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        # ============================================================
        # VPC ENDPOINT ZA S3
        # Lambda je u VPC-u bez NAT Gateway-a,
        # pa preko ovog endpoint-a može da čita S3.
        # ============================================================

        self.vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # ============================================================
        # SECURITY GROUP ZA EC2
        # ============================================================

        self.ec2_security_group = ec2.SecurityGroup(
            self,
            "EC2SecurityGroup",
            vpc=self.vpc,
            description="Security group for PostgreSQL and Superset EC2",
            allow_all_outbound=True,
        )

        # SSH
        # Za projekat ostavljamo ovako da možeš da pristupiš.
        # Produkcijski bi se ograničilo samo na tvoju IP adresu.
        self.ec2_security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(22),
            "Allow SSH",
        )

        # Superset web interfejs
        self.ec2_security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(8088),
            "Allow Superset",
        )

        # ============================================================
        # SECURITY GROUP ZA DB LOADER LAMBDU
        # ============================================================

        self.lambda_security_group = ec2.SecurityGroup(
            self,
            "DBLoaderLambdaSecurityGroup",
            vpc=self.vpc,
            description="Security group for DB Loader Lambda",
            allow_all_outbound=True,
        )

        # PostgreSQL je dostupan SAMO DB Loader Lambdi.
        self.ec2_security_group.add_ingress_rule(
            peer=self.lambda_security_group,
            connection=ec2.Port.tcp(5432),
            description="Allow DB Loader Lambda to access PostgreSQL",
        )

        # ============================================================
        # SSM ENDPOINT
        # DB Loader čita password iz Parameter Store-a.
        # ============================================================

        self.ssm_endpoint = self.vpc.add_interface_endpoint(
            "SSMEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SSM,
            subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
        )

        self.ssm_endpoint.connections.allow_from(
            self.lambda_security_group,
            ec2.Port.tcp(443),
            "Allow Lambda to access SSM Parameter Store",
        )

        # ============================================================
        # EC2 INSTANCA
        # ============================================================

        self.instance = ec2.Instance(
            self,
            "PipelineEC2",
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO,
            ),
            machine_image=ec2.MachineImage.generic_linux({
                "eu-central-1": "ami-009b038a3a0d89866"
            }),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            security_group=self.ec2_security_group,
            key_pair=ec2.KeyPair.from_key_pair_name(
                self,
                "PipelineKeyPair",
                "pipeline-key",
            ),
        )

        # ============================================================
        # OUTPUTI
        # Posle deploy-a ćeš ove vrednosti videti u terminalu.
        # ============================================================

        CfnOutput(
            self,
            "EC2PublicIP",
            value=self.instance.instance_public_ip,
            description="Public IP for SSH and Superset",
        )

        CfnOutput(
            self,
            "EC2PrivateIP",
            value=self.instance.instance_private_ip,
            description="Private IP used by DB Loader Lambda",
        )

        CfnOutput(
            self,
            "VPCId",
            value=self.vpc.vpc_id,
        )