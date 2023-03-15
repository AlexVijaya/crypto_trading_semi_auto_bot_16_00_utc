from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import os
import datetime
from upload_file_to_goggle_drive_config import folder_id
import sys
import subprocess

def run_upload_file_script(python_file_name_to_run):
    interpreter = sys.executable
    subprocess.run([interpreter, python_file_name_to_run])

def upload_files_to_google_drive(json_file_name, file_path1, file_path2):
    # Set up the authentication parameters
    path_to_dir_where_json_file_is = os.path.join(os.getcwd(), 'datasets', 'json_key_for_google')
    creds = service_account.Credentials.from_service_account_file(os.path.join(path_to_dir_where_json_file_is, json_file_name))
    service = build('drive', 'v3', credentials=creds)


    # Upload the first file

    mime_type1 = 'text/plain'
    media1 = MediaFileUpload(file_path1, mimetype=mime_type1)
    file_metadata1 = {'name': f'{file_name1}', 'parents': [f'{folder_id}']}
    file1 = service.files().create(body=file_metadata1, media_body=media1, fields='id').execute()
    print('File 1 ID:', file1.get('id'))

    # Upload the second file

    mime_type2 = 'text/plain'
    media2 = MediaFileUpload(file_path2, mimetype=mime_type2)
    file_metadata2 = {'name': f'{file_name2}', 'parents': [f'{folder_id}']}
    file2 = service.files().create(body=file_metadata2, media_body=media2, fields='id').execute()
    print('File 2 ID:', file2.get('id'))

if __name__ == '__main__':
    json_file_name = "stocks-formed-right-entry-7d1e2c6ba582.json"
    file_name1 = "crypto_" + datetime.datetime.now().strftime('%Y-%m-%d') + '.txt'
    file_name2 = f"atr_level_sl_tp_for_cryptos_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"

    file_path1 = os.path.join(os.getcwd(), 'current_rebound_breakout_and_false_breakout', file_name1)
    file_path2 = os.path.join(os.getcwd(), 'current_rebound_breakout_and_false_breakout', file_name2)
    upload_files_to_google_drive(json_file_name, file_path1, file_path2)
