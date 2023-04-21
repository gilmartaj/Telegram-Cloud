import os.path
from os.path import expanduser

import pandas as pd
import requests
import numpy as np

from telethon.sessions import StringSession
from telethon.sync import TelegramClient

from credentials import bot_token, chat_id, telethon_id, telethon_key

FILES_INFO_COLUMNS = ["file_id", "filename_we", "extension", "type", "directory", "file_size"]
FILES_INFO_CSV = ".files_info.csv"


auth = open(expanduser("~")+"/telegram-auth.txt", "r").read()
client =  TelegramClient(StringSession(auth), telethon_id, telethon_key)
client.connect()


class Cloud:
    FILES_INFO_COLUMNS = ["file_id", "filename_we", "extension", "type", "directory", "file_size"]
    FILES_INFO_CSV = ".files_info.csv"

    def __init__(self, filepath, auth, token, hashc):
        self.files_info_path = filepath
        self.files_info_df = self.read_files_info()
        self.client =  TelegramClient(StringSession(auth), token, hashc)
        self.client.connect()

    def read_files_info(self):
        try:
            return pd.read_csv(self.files_info_path, na_filter=False) if os.path.exists(self.files_info_path) else self.new_files_info_df()
        except:
            return self.new_files_info_df()

    def new_files_info_df(self):
        return pd.DataFrame(columns=FILES_INFO_COLUMNS)

    def make_directory(self, full_directory):
        if full_directory == None or full_directory=="":
            return None
        subdirs = full_directory.split("/")
        directories_df = self.new_files_info_df()
        father_dir = ""
        for i, dir in enumerate(subdirs[1:]):
            #print("dir="+dir, "farher="+father_dir)
            files_subdir_df = self.files_info_df.loc[self.files_info_df["directory"] == father_dir]
            if dir in files_subdir_df["filename_we"].unique():
                aux_df = files_subdir_df.loc[files_subdir_df["filename_we"] == dir]
                if "" in aux_df["extension"].unique():
                    if i == len(subdirs) - 2:
                        return None
            directories_df = self.update_files_info_df("0", filename_we=dir, type="directory", directory=father_dir, df=directories_df)
            father_dir = father_dir + "/" + dir
        self.files_info_df = pd.concat([self.files_info_df, directories_df]).reset_index(drop=True)
        return full_directory

    def write_files_info(self):
        self.files_info_df.to_csv(FILES_INFO_CSV, index=False)

    def upload_file(self, filepath, directory="", func = None, replace=False):
        if not self.exists_directory(directory):
            raise Exception("Directory not exists")

        filename_we, extension = os.path.splitext(filepath)
        filename_we = os.path.split(filename_we)[-1]
        
        if self.exists_file(filename_we, extension, directory):
            if replace==False:
                return None
            print(self.files_info_df.loc[(self.files_info_df["filename_we"] == filename_we) & (self.files_info_df["extension"] == extension) & (self.files_info_df["directory"] == directory)])
            self.remove_elements(self.files_info_df.loc[(self.files_info_df["filename_we"] == filename_we) & (self.files_info_df["extension"] == extension) & (self.files_info_df["directory"] == directory)].index)
        
        file_size = os.path.getsize(filepath)
        file_id = self.send_file(filepath, func)
        self.files_info_df = self.update_files_info_df(file_id, filename_we, extension, "file", directory, file_size)

        return pd.Series(data=[file_id, filename_we, extension, "file", directory, file_size], index=FILES_INFO_COLUMNS)

    def send_file_(self, filepath):
        with open(filepath, "rb") as opened_file:
            filename_we, extension = os.path.splitext(filepath)
            files = {"document": opened_file}
            method = method_by_extension(extension)
            response = requests.post(f"https://api.telegram.org/bot{bot_token}/sendDocument?chat_id={chat_id}", files=files)
            return response
            
    def remove_elements(self, index):
        self.files_info_df.drop(index, inplace=True)
    
    def exists_directory(self, directory):
        return directory == "" or self.files_info_df.loc[(self.files_info_df["filename_we"] == directory.split("/")[-1]) & (self.files_info_df["directory"] == "/".join(directory.split("/")[:-1])) & (self.files_info_df["type"] == "directory")].shape[0] > 0
        
    def exists_file(self, filename_we, extension, directory):
        return self.files_info_df.loc[(self.files_info_df["filename_we"] == filename_we) & (self.files_info_df["extension"] == extension) & (self.files_info_df["directory"] == directory)].shape[0] > 0
        
    def get_element(self, filename_we, extension, directory):
        return self.files_info_df.loc[(self.files_info_df["filename_we"] == filename_we) & (self.files_info_df["extension"] == extension) & (self.files_info_df["directory"] == directory)]
        
    def get_directory_elements(self, directory):
        return self.files_info_df.loc[self.files_info_df["directory"] == directory][["filename_we", "extension", "type"]]
          
    def rename_element(self, index, new_name):
        filename_we, extension = os.path.splitext(new_name)
        self.files_info_df.at[index,"filename_we"] = filename_we
        self.files_info_df.at[index,"extension"] = extension
        print(self.files_info_df)
        
    def get_by_index(self, index):
        return self.files_info_df.iloc[index]
    
    def send_file(self, filepath, func):
        return self.client.send_file("me", filepath,force_document=True, progress_callback = func).id

    def update_files_info_df(self, file_id, filename_we, extension="", type="file", directory="", file_size=0, df=None):
        if df is None:
            df = self.files_info_df
        new_line = pd.DataFrame([[file_id, filename_we, extension, type, directory, file_size]], columns=FILES_INFO_COLUMNS)
        return pd.concat([df, new_line]).reset_index(drop=True)

    def get_download(self, file_id):    
        response = requests.post(f"https://api.telegram.org/bot{bot_token}/getFile?file_id={file_id}")
        if response.status_code != 200 or response.json()["ok"] != True:
            return False
        file_path = response.json()["result"]["file_path"]
        download_link = f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
        return (download_link, response.json()["result"]["file_unique_id"])

    def download_file(self, file_id, func, filename=None):
        message = self.client.get_messages('me', ids=file_id)
        #message.download_media(progress_callback=func)
        from telethon.client.downloads import DownloadMethods
        from telethon.utils import get_input_location, _get_file_info
        from telethon import functions, types
        
        MB = 1048576
    
        size = message.media.document.size
        dc_id, location = get_input_location(message)
        
        total = 0
        
        if filename ==  None:
            filename = message.file.name
        
        f = open(filename, "wb")
        
        func(0, size)
        while total < size:
          result = self.client(functions.upload.GetFileRequest(
              location,
              offset=total,
              limit=MB,
              precise=True,
              cdn_supported=True
          ))
          f.write(result.bytes)
          total += len(result.bytes)
          func(total, size)
        
        f.close()