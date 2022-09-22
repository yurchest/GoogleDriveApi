import io
import threading
import time
from datetime import datetime
from dateutil.parser import parse
from operator import itemgetter
import pandas
import pandas as pd
from Google import Create_Service as create_service
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import asyncio
from threading import Thread
from multiprocessing import Process, pool
from multiprocessing import Pool, Manager

CLIENT_FILE = 'key.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']

# Retrieve file revision history
service = create_service(CLIENT_FILE, API_NAME, API_VERSION, SCOPES)


def get_file_revision_history(file_id):
    response = service.revisions().list(
        fileId=file_id,
        fields='*',
        pageSize=1000
    ).execute()

    revisions = response.get('revisions')
    nextPageToken = response.get('nextPageToken')

    while nextPageToken:
        response = service.revisions().list(
            fileId=file_id,
            fields='*',
            pageSize=1000,
            pageToken=nextPageToken
        ).execute()

        revisions = response.get('revisions')
        nextPageToken = response.get('nextPageToken')
    return revisions


def restore_file(file_id, revision_history_id, read_file_name):
    media_request = service.revisions().get_media(
        fileId=file_id,
        revisionId=revision_history_id
    )

    with io.FileIO(f"cache/{read_file_name}", 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, media_request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(read_file_name)
            print(status.progress() * 100, '%')

    media_content = MediaFileUpload(f"cache/{read_file_name}")
    body = {'name': read_file_name}
    service.files().update(
        fileId=file_id,
        media_body=media_content,
        body=body,
    ).execute()


def open_folder(query):
    response = service.files().list(q=query).execute()
    files = response.get('files')
    nextPageToken = response.get('nextPageToken')

    while nextPageToken:
        response = service.files().list(q=query).execute()
        files.extend(response.get('files'))
        nextPageToken = response.get('nextPageToken')
    return files


# file_id = '18__NduDx22Gb_m05RKPKgybBSkkcmNsd'
# revision_history = get_file_revision_history(file_id)
# # print(revision_history)
# df = pd.DataFrame(revision_history)
# df.to_csv('revision history ({0}).csv'.format(file_id), index=False)

# restore_file(file_id, revision_history[-2]['id'], revision_history[-2]["originalFilename"])


def recursive_backup(folder_id):
    print(f'Folder id = {folder_id}')
    files = open_folder(query=f"parents = '{folder_id}'")
    for file in files:
        try:
            async_func(file)
        except Exception as error:
            with open('log.txt', "a") as f:
                f.write('\n' + str(error) + '   filename :   ' + str(file['name']))



def async_func(file):
    if file['name'] in ['RyukReadMe.txt', 'hrmlog1']:
        service.files().delete(fileId=file['id']).execute()
    elif file['mimeType'] == 'application/vnd.google-apps.folder':
        print(f"Open folder {file['name']}")
        recursive_backup(file['id'])
    elif '[back.your.files@firemail.de].RYK' in file['name']:
        revision_history = get_file_revision_history(file['id'])
        revision = get_revision_without_bad(revision_history)
        restore_file(file['id'], revision['id'], revision['originalFilename'])



def get_revision_without_bad(revisions: list[dict]):
    result = []
    for i, d in enumerate(revisions):
        # if d.get('originalFilename') is None:
        #     pass
        if '[back.your.files@firemail.de].RYK' not in d['originalFilename']:
            result.append(revisions[i])
        return sorted(result, key=lambda x: parse(x['modifiedTime']).timestamp())[0]


def run_processes():
    processes = [
        Process(target=recursive_backup, args=('1-3uaxLWvwKhnYPK9mJlzbdPw_yIYg9_s',)),  # 1, 2 sem
        # Process(target=recursive_backup, args=('1xkv9yVh0AZuGEaDEuCCNXwLbEelx4wdr',)),  # 3 sem
        # Process(target=recursive_backup, args=('1yuVvmV6atPBA6EhVpq9GbMxMgTPbSf2J',)),  # 4 sem
        # Process(target=recursive_backup, args=('1yna3ufyUqmjB_digvIXZNBpgIIFH3_J-',)),  # 5 sem
        # Process(target=recursive_backup, args=('1xjZYLdDXkTr60-Z7ZCLHoiRPP6qQ0vBr',)),  # 6 sem
        # Process(target=recursive_backup, args=('1ytxw9cb42UR-WmPB4_2PvsGLt9fHWWJY',)),  # 7 sem
    ]

    for process in processes:
        process.start()

    for process in processes:
        process.join()


run_processes()
# revisions = get_file_revision_history('1CFEqb04mNv5oUIKS1dCwY1GwL_B7Ps5V')
# print(revisions[0].get('originalFilename'))
# # print(revisions[0]['originalFilename'])
# df = pandas.DataFrame(revisions)
# df.to_csv('revision history ({0}).csv'.format('1CFEqb04mNv5oUIKS1dCwY1GwL_B7Ps5V'), index=False)


## /view?usp=sharing