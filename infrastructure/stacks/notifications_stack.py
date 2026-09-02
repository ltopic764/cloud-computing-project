from aws_cdk import (
    Stack, 
    Duration,
    aws_sns as sns, # simple notification service
    aws_sns_subscriptions as subs, # who receives messages from sns
    aws_lambda as lambda_, # lambda funcion to send to discord
    aws_cloudwatch as cloudwatch, # alarms to track lambdas execution
    aws_cloudwatch_actions as cw_actions, # when alarm goes off
    aws_iam as iam, 
    aws_ssm as ssm, # ssm for reading the webhook url
    SecretValue
)
from constructs import Construct

class NotificationsStack(Stack):
    """
    Stack that creates the complete infrastructure for notifications
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        compute_stack,
        silver_stack,
        gold_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # SNS
        self.alarm_topic = sns.Topic(self, "PipelineAlarmTopic", topic_name="social-media-pipeline-alarms", display_name="Social Media Pipeline Alarms",)

        # Iam policy for the discord lambda
        # it can read the webhook url from ssm
        discord_lambda_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "ssm:GetParameter",
            ],
            resources=[
                f"arn:aws:ssm:{self.region}:{self.account}:parameter/social-media-pipeline/*"
            ],
        )

        # discord notifier lambda
        discord_lambda = lambda_.Function(
            self,
            "DiscordNotifierLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            code=lambda_.Code.from_asset("../src/utils/notifications"),
            handler="handler.handler",

            timeout=Duration.seconds(30),

            memory_size=128,

            environment={
                "LOG_LEVEL": "INFO",
                # ssm paramtere name
                "SSM_WEBHOOK_PARAM": "/social-media-pipeline/discord-webhook-url",
            },
        )

        discord_lambda.add_to_role_policy(discord_lambda_policy)

        self.alarm_topic.add_subscription(subs.LambdaSubscription(discord_lambda))

        # cloudwatch alarms
        # alarm for each lambda

        # list of lambda functions to track the status of
        lambdas_to_monitor = [
            ("HackerNewsBronze", compute_stack.hn_lambda),
            ("TwitterBronze", compute_stack.twitter_lambda),
            ("HackerNewsSilver", silver_stack.hn_silver_lambda),
            ("TwitterSilver", silver_stack.twitter_silver_lambda),
            ("HackerNewsGold", gold_stack.hn_gold_lambda),
            ("TwitterGold", gold_stack.twitter_gold_lambda),
            ("DBLoader", gold_stack.db_loader_lambda),
        ]

        for lambda_name, lambda_fn in lambdas_to_monitor:
            # create alarm
            alarm = cloudwatch.Alarm(
                self,
                f"{lambda_name}LambdaErrorAlarm",

                alarm_name = f"social-media-pipeline-{lambda_name.lower()}-errors",

                alarm_description = (
                    f"{lambda_name} Lambda function experienced an error ... "
                    f"check CloudWatch logs"
                ),

                # metrics = number of errors per lambda
                metric = lambda_fn.metric_errors(period=Duration.minutes(5),),

                threshold=1,

                evaluation_periods=1,

                # if no data, lambda did not even execute
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            )

            # when an alarm is invoked, send message to sns
            alarm.add_alarm_action(cw_actions.SnsAction(self.alarm_topic))
            