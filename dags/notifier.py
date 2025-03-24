import os
import http.client
import urllib
from airflow.notifications.basenotifier import BaseNotifier


class PushoverNotifier(BaseNotifier):
    template_fields = ("message",)

    def __init__(self, message):
        self.message = message

    def notify(self, context):
        pushover_api_token = os.getenv("PUSHOVER_API_TOKEN")
        pushover_user_key = os.getenv("PUSHOVER_USER_KEY")

        if not pushover_api_token or not pushover_user_key:
            raise ValueError(
                "Pushover API token or user token is not set in environment variables"
            )

        title = f"Airflow Task {context['task_instance'].task_id} Notification"

        conn = http.client.HTTPSConnection("api.pushover.net", 443)
        conn.request(
            "POST",
            "/1/messages.json",
            urllib.parse.urlencode(
                {
                    "token": pushover_api_token,
                    "user": pushover_user_key,
                    "message": self.message,
                    "title": title,
                }
            ),
            {"Content-type": "application/x-www-form-urlencoded"},
        )
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError(f"Pushover notification failed: {response.reason}")
