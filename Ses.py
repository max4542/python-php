import json
import boto3
from typing import List, Dict, Any, Optional
from pms import DatabaseConnection

class BulkEmailService:

    STATUS_INITIATED = '0'
    STATUS_PROCESSED = '1'
    STATUS_SUCCEED = '2'

    def __init__(self) ->None:
        """Initialize the BulkEmailService with SES client and configuration."""
        self.ses = boto3.client(
            "ses",
            region_name="us-east-1",
            endpoint_url="http://localhost:4566"
        )
        self.template = "emails_notification"
        self.config_set = "BulkEmailTracking"
        self.mail_source = "notifications@getcultureshock.com"
        self.chunk_size = 45

    def _chunk(self, recipients: List[str]):
        """Yield successive chunks from recipients list."""
        for i in range(0, len(recipients), self.chunk_size):
            yield recipients[i:i + self.chunk_size]


    def insert_logs(self, wcnx: Any, data: List[Dict[str, Any]]) -> None:
        """Bulk insert logs into the database."""
        if not data:
            return
        
        sql = '''
            INSERT INTO notification_logs
            (user_id, recipient, notification_platform, reference_id, notification_type, status, published_id)
            VALUES (%(user_id)s, %(recipient)s, %(notification_platform)s,%(reference_id)s, %(notification_type)s, %(status)s, %(published_id)s)
        '''
        with wcnx.cursor() as cursor:
            cursor.executemany(sql, data)
            wcnx.commit()

    def update_logs(self, wcnx: Any, recipients: List[str], reference_id: str, notification_platform:Optional[str], notification_type: Optional[str], published_ids: List[Optional[str]], status:str) ->None:
        """Bulk update log entries in the database."""
        if not recipients:
            return

        sql = '''
            UPDATE notification_logs
            SET status = %s,
                published_id = %s
            WHERE recipient = %s
            AND reference_id = %s
            AND notification_platform = %s
            AND notification_type = %s
        '''

        # Create a list of tuples, one per recipient
        params_list = [
            [status, published_ids[i], recipients[i], reference_id, notification_platform, notification_type]
            for i in range(len(recipients))
        ]

        print('logs', json.dumps(params_list, indent=2))

        return

        with wcnx.cursor() as cursor:
            cursor.executemany(sql, params_list)
        wcnx.commit()



    def send_bulk_mail(self, wcnx: Any, payload: Dict[str, Any]) -> str:
        """Send bulk emails using AWS SES and log the results."""
        recipients = payload['recipients']

        template_data = payload['content']
        user_id = payload.get('user_id')
        reference_id = payload.get('reference_id') or None
        notification_type = payload.get('notification_type') or None
        notification_platform = payload.get('notification_platform', None)

        for batch in self._chunk(recipients):
            batch_logs = []
            destinations = []

            for email in batch:
                batch_logs.append({
                    "recipient": email,
                    "status": self.STATUS_INITIATED,
                    "published_id": None,
                    "notification_platform": str(notification_platform),
                    "notification_type": str(notification_type),
                    "reference_id": reference_id,
                    "user_id": user_id,
                })

                destinations.append({
                    "Destination": {"ToAddresses": [email]},
                    "ReplacementTemplateData": json.dumps(template_data),
                })

            # Insert logs
            self.insert_logs(wcnx, batch_logs)


            published_ids = [None] * len(batch)
            status = self.STATUS_PROCESSED

            try:
                response = self.ses.send_bulk_templated_email(
                    Source=self.mail_source,
                    Template=self.template,
                    DefaultTemplateData=json.dumps({}),
                    Destinations=destinations,
                    ConfigurationSetName=self.config_set,
                )

                published_ids = [s.get("MessageId") if s.get("MessageId") else None for s in response.get("Status", [])]
                
                status = self.STATUS_SUCCEED

            except Exception as e:
                print("SES send error:", e)

            # Update logs with SES message IDs
            self.update_logs(
                wcnx,
                batch,
                reference_id,
                notification_platform,
                notification_type,
                published_ids,
                status
            )

        return "Emails send successfully."


def lambda_handler(event, context):
    """AWS Lambda handler to process SNS events and send bulk emails."""
    payload = json.loads(event["Records"][0]["Sns"]["Message"])
    db = DatabaseConnection()
    cnx, wcnx = db.connect()

    service = BulkEmailService()
    try:
        print(service.send_bulk_mail(wcnx, payload))
    except:
        raise
    finally:
        cnx.close()
        wcnx.close()


#test payload
if __name__ == "__main__":
    recipients = [f"user{i}@example.com" for i in range(1, 91)]
    payload = {
        "recipients": recipients,
        "content": {
            "subject": "Invoice #ORD-1002 Has Been Generated",
            "body": "Hello,\n\nYour invoice for order ORD-1002 has been successfully generated and is now available.",
            "attachment_url": "https://example.com/report.pdf",
        },
        "user_id": 42,
        "notification_platform": "0",
        "reference_id": "ORD-1111",
        "notification_type": "invoice_generated",
    }
    event = {"Records": [{"Sns": {"Message": json.dumps(payload)}}]}
    lambda_handler(event, None)
