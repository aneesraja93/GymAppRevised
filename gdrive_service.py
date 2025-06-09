import os
import pickle
from datetime import datetime

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

import database_utils as db_utils

# This scope allows the app to create files in the user's Google Drive.
# It does NOT grant permission to read or modify existing files unless created by the app.
SCOPES = ['https://www.googleapis.com/auth/drive.file']

CREDENTIALS_FILE = 'credentials.json'
TOKEN_PICKLE_FILE = 'token.pickle' # Stores user's access and refresh tokens.
BACKUP_FOLDER_NAME = 'GymApp'

def get_drive_service():
    """Gets an authorized Google Drive service object."""
    creds = None
    # The file token.pickle stores the user's access and refresh tokens.
    if os.path.exists(TOKEN_PICKLE_FILE):
        with open(TOKEN_PICKLE_FILE, 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Token refresh failed: {e}. Re-authorization is needed.")
                # If refresh fails, delete the token file to force re-auth
                if os.path.exists(TOKEN_PICKLE_FILE):
                    os.remove(TOKEN_PICKLE_FILE)
                return None # Indicate that re-authorization is required
        else:
            # This part is handled by the web flow in app.py
            # This function should only be called when a token is expected to exist.
            print("Credentials not found or invalid. Authorization is required.")
            return None

        # Save the credentials for the next run
        with open(TOKEN_PICKLE_FILE, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

def find_or_create_folder(service, folder_name):
    """Find a folder by name, or create it if it doesn't exist."""
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])

    if files:
        print(f"Found folder '{folder_name}' with ID: {files[0].get('id')}")
        return files[0].get('id')
    else:
        print(f"Folder '{folder_name}' not found, creating it...")
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        folder = service.files().create(body=file_metadata, fields='id').execute()
        print(f"Created folder with ID: {folder.get('id')}")
        return folder.get('id')

def upload_db_to_drive():
    """Performs a DB checkpoint and uploads the database file to Google Drive."""
    print("Starting database backup process...")
    try:
        # 1. Ensure database is ready for backup
        db_utils.create_checkpoint()
        print("Database checkpoint successful.")

        # 2. Get authorized Drive service
        service = get_drive_service()
        if not service:
            raise ConnectionRefusedError("Not authorized with Google Drive. Please authorize first.")

        # 3. Find or create the backup folder
        folder_id = find_or_create_folder(service, BACKUP_FOLDER_NAME)

        # 4. Prepare file for upload
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_name = f"gym_data_{timestamp}.sqlite"
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        media = MediaFileUpload(db_utils.DB_PATH, mimetype='application/x-sqlite3', resumable=True)

        # 5. Upload the file
        print(f"Uploading '{file_name}' to Google Drive...")
        request = service.files().create(body=file_metadata, media_body=media, fields='id')
        response = None
        # resumable upload logic
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"Uploaded {int(status.progress() * 100)}%")
        
        print(f"File upload complete. File ID: {response.get('id')}")
        return {"success": True, "message": f"Successfully backed up as '{file_name}'."}

    except Exception as e:
        print(f"An error occurred during backup: {e}")
        return {"success": False, "message": str(e)}