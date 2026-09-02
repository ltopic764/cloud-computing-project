from aws_cdk import (
    Stack,
    Duration,
    aws_stepfunctions as sfn, # stepfunction service
    aws_stepfunctions_tasks as tasks, # task that a step function can call
    aws_events as events, # event bridge that triggers state machine
    aws_events_targets as targets, 
    aws_iam as iam,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
)
from constructs import Construct

class StepFunctionsStack(Stack):
    """
    Stack that creates Step Functions State Machine for Silver Layer 

    Automatically run everyday at 03:00 UTC, after bronze lambda retreives its data
    """
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        silver_stack,
        gold_stack,
        notifications_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # SILVER STATE MACHINE
        # HN Silver Lambda call task
        hn_silver_task = tasks.LambdaInvoke(
            self,
            "NormalizeHackerNewsData",
            # which lambda is called
            lambda_function=silver_stack.hn_silver_lambda,
            payload=sfn.TaskInput.from_object({}), # yesterday
            # if it fails, try again
            retry_on_service_exceptions=True,
            output_path="$.Payload",
            comment="Normalization of HN Bronze data in Silver Parquet format",
        )

        # Twitter Silver Lambda call task
        twitter_silver_task = tasks.LambdaInvoke(
            self,
            "NormalizeTwitterData",
            lambda_function=silver_stack.twitter_silver_lambda,
            payload=sfn.TaskInput.from_object({
                "run_date": "2026-05-29"
            }),
            retry_on_service_exceptions=True,
            output_path="$.Payload",
            comment="Normalization of Twitter Bronze data in Silver Parquet format",
        )

        # Parallel state
        parallel_normalization = sfn.Parallel(
            self,
            "ParallelShema",
            comment="Parallel normalization of both HN and Twitter data",
        )

        parallel_normalization.branch(hn_silver_task)
        parallel_normalization.branch(twitter_silver_task)

        # State machine
        silver_state_machine = sfn.StateMachine(
            self,
            "SilverLayerStateMachine",
            state_machine_name="social-media-pipeline-silver-normalization",
            definition_body=sfn.DefinitionBody.from_chainable(
                parallel_normalization
            ),
            # max execute time
            timeout=Duration.minutes(15),
            state_machine_type=sfn.StateMachineType.STANDARD,
            logs=sfn.LogOptions(
                destination=self._create_log_group("silver"),
                level=sfn.LogLevel.ERROR,
                include_execution_data=True,
            ),
        )

        # Event bridge shceduler
        events.Rule(
            self,
            "SilverLayerSchedule",
            schedule=events.Schedule.cron(minute="0", hour="3"),
            description="Begin Silver State Machine every day at 03:00 UTC",
        ).add_target(
            targets.SfnStateMachine(
                silver_state_machine,
                input=events.RuleTargetInput.from_object({}),
            )
        )

        # GOLD STATE MACHINE
        # Parallel execution of both gold lambdas and then when both finished goes the DB Loader

        hn_gold_task = tasks.LambdaInvoke(
            self,
            "TransformHackerNewsData",
            lambda_function=gold_stack.hn_gold_lambda,
            payload=sfn.TaskInput.from_object({}),
            retry_on_service_exceptions=True,
            output_path="$.Payload",
            comment="Calculating HN gold metrics from silver parquets",
        )

        twitter_gold_task = tasks.LambdaInvoke(
            self, 
            "TransformTwitterData",
            lambda_function=gold_stack.twitter_gold_lambda,
            payload=sfn.TaskInput.from_object({"run_date": "2026-05-29"}),
            retry_on_service_exceptions=True,
            output_path="$.Payload",
            comment="Calculating Twitter gold metrics from silver parquets",
        )

        # waits for both of the lambdas
        db_loader_task = tasks.LambdaInvoke(
            self, 
            "FillPostgreSQL",
            lambda_function=gold_stack.db_loader_lambda,
            retry_on_service_exceptions=True,
            output_path="$.Payload",
            comment="Move gold parquet data to PostgreSQL on EC2",
        )

        # parallel state for gold
        parallel_gold = sfn.Parallel(
            self,
            "ParallelTransofmration",
            comment="Parallel couunting HN and Twitter gold metrics",
        )
        parallel_gold.branch(hn_gold_task)
        parallel_gold.branch(twitter_gold_task)

        gold_chain = parallel_gold.next(db_loader_task)

        gold_state_machine = sfn.StateMachine(
            self,
            "GoldLayerStateMachine",
            state_machine_name="social-media-pipeline-gold-transformation",
            definition_body=sfn.DefinitionBody.from_chainable(gold_chain),
            timeout=Duration.minutes(20),
            state_machine_type=sfn.StateMachineType.STANDARD,
            logs=sfn.LogOptions(
                destination=self._create_log_group("gold"),
                level=sfn.LogLevel.ERROR,
                include_execution_data=True,
            ),
        )

        events.Rule(
            self,
            "GoldLayerSchedule",
            schedule=events.Schedule.cron(minute="0", hour="4"),
            description="Begin Gold State Machine every day at 04:00 UTC",
        ).add_target(
            targets.SfnStateMachine(
                gold_state_machine,
                input=events.RuleTargetInput.from_object({}),
            )
        )

        # CloudWatch alarms for both
        for sm_name, sm in [
                ("Silver", silver_state_machine),
                ("Gold", gold_state_machine),
            ]:
                alarm = cloudwatch.Alarm(
                    self,
                    f"{sm_name}StateMachineFailureAlarm",
                    alarm_name=f"social-media-pipeline-{sm_name.lower()}-state-machine-failures",
                    alarm_description=f"{sm_name} State Machine failed",
                    metric=sm.metric_failed(period=Duration.minutes(5)),
                    threshold=1,
                    evaluation_periods=1,
                    treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
                )
                alarm.add_alarm_action(
                    cw_actions.SnsAction(notifications_stack.alarm_topic)
                )

    def _create_log_group(self, layer: str):
        from aws_cdk import aws_logs as logs
        return logs.LogGroup(
            self,
            f"{layer.capitalize()}StateMachineLogGroup",
            log_group_name=f"/aws/states/social-media-pipeline-{layer}",
            retention=logs.RetentionDays.ONE_WEEK,
        )
