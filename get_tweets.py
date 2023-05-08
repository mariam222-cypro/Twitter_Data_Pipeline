# # Databricks notebook source
# import requests
# import os
# import json
# import csv
# import datetime
# import dateutil.parser
# import unicodedata
# import time
# import socket 
# from datetime import datetime, timedelta, timezone

# os.environ['TOKEN'] = "AAAAAAAAAAAAAAAAAAAAAJVCcgEAAAAAOq3ylzTQ3Jr4Fa81CW69E57Sdwk%3Dcbhfz4sTmmo116Uay20mgWEOsspvy057SwwkDcwzeyvNrh0om8"
# def auth():
#     return os.getenv('TOKEN')

# def create_headers(bearer_token):
#     headers = {"Authorization": "Bearer {}".format(bearer_token)}
#     return headers

# def create_url(keyword, start_date, end_date, max_results = 10):
    
#     search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

#     #change params based on the endpoint you are using
#     query_params = {'query': keyword,
#                     'start_time': start_date,
#                     'end_time': end_date,
#                     'max_results': max_results,
#                     'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
#                     'tweet.fields': 'id,text,author_id,conversation_id,created_at,lang,public_metrics,source',
#                     'user.fields': 'id,name,username,created_at,description,public_metrics,verified,location',
#                     'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
#                     'next_token': {}}
#     return (search_url, query_params)

# def connect_to_endpoint(url, headers, params, next_token = None):
#     params['next_token'] = next_token   #params object received from create_url function
#     response = requests.request("GET", url, headers = headers, params = params)
#     print("Endpoint Response Code: " + str(response.status_code))
#     if response.status_code != 200:
#         raise Exception(response.status_code, response.text)
#     return response.json()


# bearer_token = auth()
# headers = create_headers(bearer_token)
# keywords = "(Ukraine Russia) OR (Crimea Russia) OR (Donetsk Russia) OR (Luhansk Russia) OR (Donbas Russia) OR (Novorossiya) OR (Azov Russia) OR (Minsk agreement) OR (Russia) OR (War Russia Ukraine) OR (Putin)"
# hashtags = ["#Ukraine", "#Russia", "#Crimea", "#MinskAgreement", "#Maidan", "#Euromaidan", "#annexation", "#WarInUkraine", "#RussiaUkraineConflict", "#UkraineConflict", "#Putin", "#RussiaInvasion", "#RussianAggression", "#StopRussianAggression", "#SaveUkraine", "#PrayForUkraine", "#PeaceForUkraine", "#annexation", "#sanctions"]
# keyword = ' '.join([keywords] + hashtags) 


# s = socket.socket()
# host = "127.0.0.1"
# port = 7773

# # COMMAND ----------

# s.bind((host, port))

# # COMMAND ----------

# print("Listening on port: %s" % str(port))
# s.listen(5)
# clientsocket, address = s.accept()
# socket_is_closed = False 
# try:
#     print("Received request from: " + str(address)," connection created.")
#     while True:
#         start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=120)
#     start_time = start_time.isoformat(timespec='milliseconds') + 'Z'
#     end_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
#     end_time = end_time.isoformat(timespec='milliseconds') + 'Z'
#         max_results = 100
#         url = create_url(keywords, start_time, end_time, max_results)
#         json_response = connect_to_endpoint(url[0], headers, url[1])
#         for data in json_response['data']:
#             print("Sending tweet data:" +  json.dumps(data))
#             try:
#                 oneLineData = json.dumps(data)
#                 oneLineData = oneLineData + '\n'
#                 clientsocket.send(oneLineData.encode('utf-8'))
#             except BrokenPipeError:
#                 print("Client disconnected.")
#                 clientsocket.close()
#                 socket_is_closed = True 
#                 break
#         if socket_is_closed:
#             clientsocket, address = s.accept()
#         next_token = json_response['meta']['next_token']
#         url = create_url(keywords, start_time, end_time, max_results)
#         url[1]['next_token'] = next_token
#         time.sleep(5)
#         print("New Streaming")
# except Exception as e:
#     print("Error:", e)
#     clientsocket.close()

# Databricks notebook source
import requests
import os
import json
import csv
import datetime
import dateutil.parser
import unicodedata
import time
import socket 
from datetime import datetime, timedelta, timezone

os.environ['TOKEN'] = "AAAAAAAAAAAAAAAAAAAAAJVCcgEAAAAAOq3ylzTQ3Jr4Fa81CW69E57Sdwk%3Dcbhfz4sTmmo116Uay20mgWEOsspvy057SwwkDcwzeyvNrh0om8"
def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,conversation_id,created_at,lang,public_metrics,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified,location',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


bearer_token = auth()
headers = create_headers(bearer_token)
keywords = "(Ukraine Russia) OR (Crimea Russia) OR (Donetsk Russia) OR (Luhansk Russia) OR (Donbas Russia) OR (Novorossiya) OR (Azov Russia) OR (Minsk agreement) OR (Russia) OR (War Russia Ukraine) OR (Putin)"
hashtags = ["#Ukraine", "#Russia", "#Crimea", "#MinskAgreement", "#Maidan", "#Euromaidan", "#annexation", "#WarInUkraine", "#RussiaUkraineConflict", "#UkraineConflict", "#Putin", "#RussiaInvasion", "#RussianAggression", "#StopRussianAggression", "#SaveUkraine", "#PrayForUkraine", "#PeaceForUkraine", "#annexation", "#sanctions"]
keyword = ' '.join([keywords] + hashtags) 


s = socket.socket()
host = "127.0.0.1"
port = 7777

# COMMAND ----------

s.bind((host, port))

# COMMAND ----------

print("Listening on port: %s" % str(port))
s.listen(5)
clientsocket, address = s.accept()
socket_is_closed = False 
try:
    print("Received request from: " + str(address)," connection created.")
    while True:
        start_time = "2023-05-01T00:00:00.000Z"
        end_time = "2023-05-03T00:00:00.000Z"
        max_results = 100
        url = create_url(keywords, start_time, end_time, max_results)
        json_response = connect_to_endpoint(url[0], headers, url[1])
        for data in json_response['data']:
            print("Sending tweet data:" +  json.dumps(data))
            try:
                author_id = data['author_id']
                user = next((user for user in json_response['includes']['users'] if user['id'] == author_id), None)
                if user is not None:
                    data['user'] = user
                    oneLineData = json.dumps(data)
                    oneLineData = oneLineData + '\n'
                    clientsocket.send(oneLineData.encode('utf-8'))
            except BrokenPipeError:
                print("Client disconnected.")
                clientsocket.close()
                socket_is_closed = True 
                break
        if socket_is_closed:
            clientsocket, address = s.accept()
        next_token = json_response['meta']['next_token']
        url = create_url(keywords, start_time, end_time, max_results)
        url[1]['next_token'] = next_token
        time.sleep(5)
        print("New Streaming")
except Exception as e:
    print("Error:", e)
    clientsocket.close()