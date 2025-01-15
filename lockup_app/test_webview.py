import gspread as gs
import webview
import os
import lockup_scraper
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from json import dumps

class Api():  
    def __init__(self):
        self.service_account = 'lockup_app/credentials/service_account_credentials.json'
        self.gc = gs.service_account(filename='lockup_app/credentials/service_account_credentials.json')
        self.files = None
        self.gid = None
    
    def google_auth(self, method = "service_account"):
        if method == "service_account":
            self.gc = gs.service_account(filename='lockup_app/credentials/service_account_credentials.json')
        elif method == "oauth":
            self.gc = gs.oauth(
                credentials_filename='lockup_app/credentials/oauth_credentials.json',
                authorized_user_filename='lockup_app/credentials/authorized_user.json'
            )

    def google_deauth(self):
        if os.path.exists('lockup_app/credentials/authorized_user.json'):
            os.remove('lockup_app/credentials/authorized_user.json')  
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

    def go_extract(self):
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
                    #write to file
                    json_str = dumps(files)
                    js_code = f"let json_data = `{json_str}`"

                    with open("lockup_app/drive_search.js", "w+") as outfile:
                        outfile.write(js_code)
                    break

        except HttpError as error:
            print(f"An error occurred: {error}")
            files = None

if __name__ == '__main__':
    api = Api()
    window = webview.create_window('App', 'index.html', js_api=api)

    window.events.closed += api.google_deauth

    webview.start(api.search_file, icon="static/ida_b_free_data.png")

#TODO create functionality to select sheet
#TODO show file name
#TODO add create sheet function
#TODO test multiple pdf's at once
#TODO add ready indicator; tests after each previous step
#TODO comment and add docstrings
#TODO add screen to give time for the app shit to load to fix the bug of needing to restart