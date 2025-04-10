import json
import re
from datetime import datetime

import google
from dateutil import tz

from google.cloud import bigquery
from googleapiclient import discovery
from google.oauth2 import credentials
from google.oauth2 import service_account


class BigQueryConnectionExample:

    def __init__(self, project, credentials):
        super().__init__()

        self.project          = project
        self.credentials      = credentials

        self.bigquery_clients = {}
        self.project_client   = discovery.build("cloudresourcemanager", "v1", credentials=credentials)

    @staticmethod
    def generate_credentials_from_dict(user, credentials_dict, project):
        if BigQueryConnectionExample.is_service_account_username(user):
            print("Creating service account credentials for user " + user)
            return service_account.Credentials.from_service_account_info(credentials_dict)
        else:
            print("Creating user credentials for user " + user)
            ret = credentials.Credentials(
                # token=credentials_dict['token'],
                token=None,
                client_id=credentials_dict['client_id'],
                client_secret=credentials_dict['client_secret'],
                quota_project_id=project,
                refresh_token=credentials_dict['refresh_token'],
                token_uri="https://oauth2.googleapis.com/token"
            )

            # Step 2: Refresh to get a fresh access token
            auth_req = google.auth.transport.requests.Request()
            ret.refresh(auth_req)

            print("Access token: " + ret.token)

            return ret

    @staticmethod
    def is_service_account_username(user):
        """
            Determines if a username corresponds to a service account.
            Modify this logic according to your username conventions.
            """
        if not isinstance(user, str):
            return False

        if "_sa" in user:
            return True

        return re.search(r"_sa\d+$", user) is not None

    def get_bigquery_client(self, project_id):
        if project_id not in self.bigquery_clients:
            self.bigquery_clients[project_id] = bigquery.Client(project=project_id, credentials=self.credentials)

        return self.bigquery_clients[project_id]

    def get_access_token(self):

        # Refreshing token if expired
        from_zone = tz.tzutc()
        to_zone   = tz.tzlocal()

        current_time = datetime.now()
        current_time = current_time.astimezone(to_zone)

        if getattr(self, 'expiry_time', None) is None or current_time >= self.expiry_time:
            auth_req = google.auth.transport.requests.Request()
            self.credentials.refresh(auth_req)

            # Tell the datetime object that it's in UTC time zone since
            # datetime objects are 'naive' by default
            utc = self.credentials.expiry.replace(tzinfo=from_zone)

            # Convert time zone
            self.expiry_time = utc.astimezone(to_zone)

        return self.credentials.token


    def execute_sql_query(self, query):

        ret = None
        try:
            print("Executing Query : " + query)

            df = (
                self.get_bigquery_client(self.project).query(query)
                    .result()
                    .to_dataframe(
                    # Optionally, explicitly request to use the BigQuery Storage API. As of
                    # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
                    # API is used by default.
                    create_bqstorage_client=True,
                )
            )

            print(df)

        except Exception as e:
            raise


USER_EMAIL = "<your-email>"
PROJECT_ID = "<your-project-id>"

creds = BigQueryConnectionExample.generate_credentials_from_dict(
    USER_EMAIL,
    json.load(open('authorized_user.json', 'r')),
    PROJECT_ID
)

connector = BigQueryConnectionExample(
    project=PROJECT_ID,
    credentials=creds
)

# Example SQL query
sql_query = "SELECT * FROM `<your-project-id>.<your-dataset-name>.<your-table-name>` LIMIT 10"

connector.execute_sql_query(sql_query)