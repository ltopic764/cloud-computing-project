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
        notifications_stack,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

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
        state_machine = sfn.StateMachine(
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
                destination=self._create_log_group(),
                level=sfn.LogLevel.ERROR,
                include_execution_data=True,
            ),
        )

        # Event bridge shceduler
        silver_schedule = events.Rule(
            self,
            "SilverLayerSchedule",
            # cron
            schedule=events.Schedule.cron(
                minute="0",
                hour="3",
            ),
        )

        silver_schedule.add_target(
            targets.SfnStateMachine(
                state_machine,
                input=events.RuleTargetInput.from_object({}), # no run date passed, default is yesterday
            )
        )

        # CloudWatch alarm for state machine error
        state_machine_alarm = cloudwatch.Alarm(
            self,
            "SilverStateMachineFailureAlarm",
            alarm_name="social-pipeline-silver-state-machine-failures",
            alarm_description=(
                "Silver Layer State Machine failed - "
                "data normalization did not succeed. "
                "Check CloudWatch Logs for details "
            ),
            metric=state_machine.metric_failed(
                period=Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        state_machine_alarm.add_alarm_action(
            cw_actions.SnsAction(notifications_stack.alarm_topic)
        )

    def _create_log_group(self):
        from aws_cdk import aws_logs as logs

        return logs.LogGroup(
            self,
            "SilverStateMachineLogGroup",
            log_group_name="/aws/states/social-media-pipeline-silver",
            retention=logs.RetentionDays.ONE_WEEK,
        )
