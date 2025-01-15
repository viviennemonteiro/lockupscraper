import gspread as gs
import webview
import os
import lockup_scraper
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class Api():  
    def __init__(self):
        self.service_account = 'credential/service_account_credentials.json'
        self.gc = gs.service_account(filename=self.service_account)
        self.files = None
        self.gid = None
    
    def google_auth(self, method = "service_account"):
        if method == "service_account":
            self.gc = gs.service_account(filename=self.service_account)
        elif method == "oauth":
            self.gc = gs.oauth(
                credentials_filename='credential/oauth_credentials.json',
                authorized_user_filename='credential/authorized_user.json'
            )

    def google_deauth(self):
        if os.path.exists('credential/authorized_user.json'):
            os.remove('credential/authorized_user.json')  
            self.gc = None
        else:
            print("No auth to delete")

    def open_file_dialog(self):
        file_types = ('PDF Files (*.pdf)', 'All files (*.*)')

        result = window.create_file_dialog(
            webview.OPEN_DIALOG, allow_multiple=False, file_types=file_types
        )
        print(result)
        self.files = result

    def go_upload(self):
        print(self.files)
        for doc in self.files:
            df = lockup_scraper.scrape_fulldoc(doc)
            lockup_scraper.append_to_sheet(self.gc, df, "test sheet", "test tab")

    def search_file(self):
        """
        Search file in drive location
        """
        SCOPES = ['https://www.googleapis.com/auth/drive']
        try:
            # create drive api client
            credentials = service_account.Credentials.from_service_account_file(self.service_account, scopes=SCOPES)
            service = build('drive', 'v3', credentials=credentials)
            files = []
            page_token = None
            while True:
            # pylint: disable=maybe-no-member
                response = (
                    service.files()
                    .list(
                        q="mimeType='application/vnd.google-apps.spreadsheet'",
                        spaces="drive",
                        fields="nextPageToken, files(id, name)",
                        pageToken=page_token,
                    )
                    .execute()
                )
                for file in response.get("files", []):
                    # Process change
                    print(f'Found file: {file.get("name")}, {file.get("id")}')
                files.extend(response.get("files", []))
                page_token = response.get("nextPageToken", None)
                if page_token is None:
                    break

        except HttpError as error:
            print(f"An error occurred: {error}")
            files = None

        with open("drive_search.json", "w") as outfile:
            json.dump(files, outfile)

api = Api()