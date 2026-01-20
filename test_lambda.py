import json
import boto3
import mysql.connector



class DatabaseConnection:
    def __init__(self):
        self.cnx = None
        self.wcnx = None

    def connect(self):
        # Read connection (read replica / normal read)
        self.cnx = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Alina123@",
            database="voxship",
            autocommit=True,
        )

        # Write connection
        self.wcnx = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Alina123@",
            database="voxship",
            autocommit=True,
        )

        return self.cnx, self.wcnx

    def close(self):
        if self.cnx and self.cnx.is_connected():
            self.cnx.close()

        if self.wcnx and self.wcnx.is_connected():
            self.wcnx.close()


class BulkEmailService:

    STATUS_INITIATED = '0'
    STATUS_PROCESSED = '1'
    STATUS_SUCCEED = '2'

    def __init__(self):
        self.ses = boto3.client(
            "ses",
            region_name="us-east-1",
            endpoint_url="http://localhost:4566"
        )
        self.template = "emails_notification"
        self.config_set = "BulkEmailTracking"
        self.mail_source = "notifications@getcultureshock.com"
        self.chunk_size = 45

    def _chunk(self, recipients):
        for i in range(0, len(recipients), self.chunk_size):
            yield recipients[i:i + self.chunk_size]

    def insert_logs(self, wcnx, data):

        """Bulk insert log entries into the database."""
        if not data:
            return
        
        sql = '''
            INSERT INTO notification_logs
            (user_id, recipient, notification_platform, notification_type, status, published_id)
            VALUES (%(user_id)s, %(recipient)s, %(notification_platform)s, %(notification_type)s, %(status)s, %(published_id)s)
        '''
        with wcnx.cursor() as cursor:
            cursor.executemany(sql, data)
            wcnx.commit()

    def send_bulk_mail(self, wcnx, payload):
        recipients = payload['recipients']
        template_data = payload['data']
        user_id = payload.get('user_id')
        notification_type = payload.get('notification_type', None)
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
                    "user_id": user_id,
                })

                destinations.append({
                    "Destination": {"ToAddresses": [email]},
                    "ReplacementTemplateData": json.dumps(template_data),
                })

            try:
                response = self.ses.send_bulk_templated_email(
                    Source=self.mail_source,
                    Template=self.template,
                    DefaultTemplateData=json.dumps(template_data),
                    Destinations=destinations,
                    ConfigurationSetName=self.config_set,
                )

                # Update logs with SES message IDs
                for i, status in enumerate(response.get("Status", [])):
                    batch_logs[i]["published_id"] = status.get("MessageId") or None
                    batch_logs[i]["status"] = self.STATUS_SUCCEED

            except Exception as e:
                print("SES send error:", e)

                for log in batch_logs:
                    log["status"] = self.STATUS_PROCESSED
                    log["published_id"] = None

            # Insert logs
            self.insert_logs(wcnx, batch_logs)

        return "Emails processed successfully."


def lambda_handler(event, context):
    payload = json.loads(event["Records"][0]["Sns"]["Message"])
    db = DatabaseConnection()
    cnx, wcnx = db.connect()

    service = BulkEmailService()
    try:
        
        result = service.send_bulk_mail(wcnx, payload)
        print(result)
    finally:
        cnx.close()
        wcnx.close()


# # Example test payload
# if __name__ == "__main__":
#     payload = {
#         "recipients": ["maxrai788@gmail.com", "alina12@gmail.com", "steve12@gmail.com"],
#         "data": {
#             "notification_type": "Invoice_create",
#             "subject": "Invoice #ORD-1002 Has Been Generated",
#             "body": "Hello,\n\nYour invoice for order ORD-1002 has been successfully generated and is now available.",
#             "attachment_url": "https://example.com/report.pdf",
#         },
#         "user_id": 42,
#         "notification_platform": "0"
#     }
#     event = {"Records": [{"Sns": {"Message": json.dumps(payload)}}]}
#     lambda_handler(event, None)
