import json
import logging
import os
import boto3
import urllib.request

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

SSM_WEBHOOK_PARAM = os.environ.get("SSM_WEBHOOK_PARAM", "/social-media-pipeline/discord-webhook-url")

ssm_client = boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "eu-central-1"))


def get_webhook_url() -> str:
    """
    Get Discord webhook url from ssm parameter store
    """
    response = ssm_client.get_parameter(
        Name=SSM_WEBHOOK_PARAM,
        WithDecryption=True,
    )
    return response["Parameter"]["Value"]

def parse_sns_message(event: dict) -> dict:
    """
    Get Message from sns event
    """
    sns_record = event["Records"][0]["Sns"]

    # subject is the title for discord alert
    subject = sns_record.get("Subject", "AWS Alarm")

    # message is in JSON format
    try:
        message = json.loads(sns_record["Message"])
    except json.JSONDecodeError:
        message = {"details": sns_record["Message"]}

    function_name = None
    trigger = message.get("Trigger", {})
    dimensions = trigger.get("Dimensions", [])

    for dim in dimensions:
        if dim.get("name") == "FunctionName":
            function_name = dim.get("value")
            break
    
    return {
        "subject": subject,
        "alarm_name": message.get("AlarmName", "Unknown alarm"),
        "alarm_description": message.get("AlarmDescription", ""),
        "new_state": message.get("NewStateValue", "ALARM"),
        "old_state": message.get("OldStateValue", "OK"),
        "reason": message.get("NewStateReason", "No details"),
        "region": message.get("Region", "eu-central-1"),
        "timestamp": message.get("StateChangeTime", ""),
        "function_name": function_name,
    }

# helper
def build_logs_url(function_name: str, region: str) -> str:
    """
    Build a link to CloudWatch logs for the lambda function
    """
    log_group = f"/aws/lambda/{function_name}"

    # URL-encode log group path
    encoded_log_group = log_group.replace("/", "$252F")

    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home"
        f"?region={region}#logsV2:log-groups/log-group/{encoded_log_group}"
    )

def build_discord_message(parsed: dict) -> dict:
    if parsed["new_state"] == "ALARM":
        color = 0xFF0000
    else:
        color = 0x00FF00

    region_code = os.environ.get("AWS_REGION", "eu-central-1")

    fields = [
        {
            "name": "New state",
            "value": parsed["new_state"],
            "inline": True,
        },
        {
            "name": "Previous state",
            "value": parsed["old_state"],
            "inline": True,
        },
        {
            "name": "Time",
            "value": parsed["timestamp"],
            "inline": True,
        }
    ]

    if parsed["function_name"]:
        logs_url = build_logs_url(parsed["function_name"], region_code)
        fields.append({
            "name": "Error details",
            "value": f"[Open CloudWatch Logs]({logs_url})",
            "inline": False,
        })
    else:
        # fallback
        fields.append({
            "name": "Reason",
            "value": parsed["reason"],
            "inline": False,
        })
    
    return {
        "embeds": [
            {
                "title": f"{parsed['alarm_name']}",
                "description": parsed["alarm_description"],
                "color": color,
                "fields": fields,
                "footer": {
                    "text": "Social media pipeline - AWS CloudWatch"
                }

            }
        ]
    }

    
def send_to_discord(webhook_url: str, message: dict) -> bool:
    data = json.dumps(message).encode("utf-8")

    # create HTTP req
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "SocialMediaPipeline/1.0",
        },
        method="POST",
    )

    try :
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 204:
                logger.info("Successfully sent message to Discord")
                return True
            else:
                logger.info(f"Unexpected Discord status code: {response.status}")
                return False
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        return False
    
def handler(event: dict, context) -> dict:
    logger.info(f"RAW EVENT: {json.dumps(event)}")  # TEMP debug

    try:
        # parse sns message
        parsed = parse_sns_message(event)
        logger.info(f"Alarm: {parsed['alarm_name']}, state: {parsed['new_state']}")

        # get webhook url
        webhook_url = get_webhook_url()

        # build discord message
        discord_message = build_discord_message(parsed)

        # send message
        success = send_to_discord(webhook_url, discord_message)

        if success:
            return {"statusCode": 200, "message": "Notification sent"}
        else:
            return {"statusCode": 500, "message": "Sending failed"}

    except Exception as e:
        logger.error(f"Error in Discord Notifier lambda: {e}", exc_info=True) 
        raise
