import pandas as pd
import time
import pandas_gbq
import requests
from google.cloud import bigquery
from google.oauth2 import service_account



class Extract:
    def __init__(self, token, channel_id, after=None):
        self.headers = {'Authorization': token}
        self.base_url = f'https://discord.com/api/v9/channels/{channel_id}/messages'
        self.after = after
        self.project_id = 'devpw-376620'
        self.table_id = 'Pw_events.pw_dev'
        self.dataset_id = 'pw_dev'

    def get_data(self):
        r = requests.get(self.base_url, headers=self.headers, params={"limit":100, "after":self.after})
        return r.json()

    @staticmethod
    def setup_gcp_credentials():
        service_account.Credentials.from_service_account_file("GBQPW.json",scopes=["https://www.googleapis.com/auth/cloud-plataform", 'https://www.googleapis.com/auth/drive'])

    def process_data(self, data):
        processed_data = []
        for dado in data:
            content = dado.get("content").replace("\r", "")
            timestamp = dado.get("timestamp")
            id = dado.get("id")

            if content.count("matou") > 1:
                contents = content.split("** **")
                for c in contents:
                    c = c.replace("*", "").split(" matou ")
                    c.append(timestamp)
                    c.append(id)
                    processed_data.append(c)
            else:
                content = content.replace("*", "").split(" matou ")
                content.append(timestamp)
                content.append(id)
                processed_data.append(content)
        return processed_data

    @staticmethod
    def create_dataframe(data):
        df = pd.DataFrame(data, columns=["Killer","Victim","Time", "id"])                     
        df = df.dropna(subset=['id'])
        df = df.astype({'id':'int64'})
        df['Time'] = pd.to_datetime(df['Time'], errors='coerce')
        df['Time'] = df['Time'].dt.tz_convert('America/Sao_Paulo')
        # df['Date'] = df['Time'].apply(lambda x: x.strftime('%Y-%m-%d').encode('utf-8'))
        # df['Hour_Minute'] = df['Time'].dt.strftime('%H:%M')
        return df

    def start(self):
        Extract.setup_gcp_credentials()
        while True:
            data = self.get_data()
            if len(data) == 0:
                print('Nenhum dado novo...')
                time.sleep(45)
                continue

            last_id = data[0].get('id')
            data = self.process_data(data)
            df = Extract.create_dataframe(data)  
            pandas_gbq.to_gbq(df, self.table_id, project_id=self.project_id, if_exists='append')
            self.after = last_id
            time.sleep(5)

token = 'MjEwNTM3NjY1MDY4NTMxNzEy.GX2UU1.lfhgZGF08L5teWb2b51QeUxRtiX9OK71ABTYfE'
channel_id='1035423433040875590'
extract = Extract(token, channel_id,after=1112168911987810307)
extract.start()


##ultimo = 1112186692162551898
##inicio = 1112168911987810307