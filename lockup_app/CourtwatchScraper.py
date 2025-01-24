import os
from json import dumps
import LU_scraper
import gspread as gs
import webview
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError



class Api():  
    def __init__(self):
        self.service_account = 'credentials/service_account_credentials.json'
        self.gc = gs.service_account(filename='credentials/service_account_credentials.json')
        self.lockup_list = None
        self.gid = None
    
    def log(self, message):
        print(message)

    def google_auth(self, method = "service_account"):
        if method == "service_account":
            self.gc = gs.service_account(filename='credentials/service_account_credentials.json')
        elif method == "oauth":
            self.gc = gs.oauth(
                credentials_filename='credentials/oauth_credentials.json',
                authorized_user_filename='credentials/authorized_user.json'
            )

    def google_deauth(self):
        if os.path.exists('credentials/authorized_user.json'):
            os.remove('credentials/authorized_user.json')  
            self.gc = None
        else:
            print("No auth to delete")
    
    def delete_search_history(self):
        if os.path.exists('drive_search.json'):
            os.remove('drive_search.json')  
            self.gc = None
        else:
            print("No search history to delete")

    def open_file_dialog(self):
        file_types = ('PDF Files (*.pdf)', 'All files (*.*)')
        result = window.create_file_dialog(
            webview.OPEN_DIALOG, allow_multiple=True, file_types=file_types
        )
        print(result)
        self.lockup_list = list(result)
        
        display = window.evaluate_js(f"""
                                    document.getElementById('file_display').innerHTML = '{", ".join(result)}'
                                    is_lockup_selected = true
                                    check_ready()""")
        print(display)

    def go_extract(self, gid):
        for doc in self.lockup_list:
            df = LU_scraper.scrape_fulldoc(doc, quiet=False)
            LU_scraper.append_to_sheet(self.gc, df, gid)
            print(f"Uploaded: {doc}")
        display = window.evaluate_js("""
                                     document.getElementById('done_flag').innerHTML = 'DONE!!!'
                                     document.getElementById("ready").innerHTML = "Not Ready"
                                     is_lockup_selected = false
                                     """)
        self.lockup_list = None
        print(display)


    def search_drive(self):
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

                    with open("drive_search.js", "w+") as outfile:
                        outfile.write(js_code)
                    break

        except HttpError as error:
            print(f"An error occurred: {error}")
            files = None

if __name__ == '__main__':
    api = Api()
    window = webview.create_window('App', 'index.html', js_api=api)

    window.events.closed += api.google_deauth
    window.events.closed += api.delete_search_history

    webview.start(api.search_drive, icon="static/ida_b_free_data.png", debug=True)

#TODO add create sheet function
#TODO comment and add docstrings
#TODO add screen to give time for the app shit to load to fix the bug of needing to restart