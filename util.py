import time
import yaml
import os
import base64
import requests
import sys,json
from sys import stdout
from concurrent.futures import ThreadPoolExecutor
from prettytable import PrettyTable
from docopt import docopt
from dateutil import parser
import math
import pandas as pd
import numpy as np
from snap_pull import args
import logging

logging.basicConfig(level=logging.INFO,  filename='logs.log', filemode='a', format='%(user)s - %(message)s  %(asctime)s', datefmt="%Y-%m-%d %T")


requests.packages.urllib3.disable_warnings()

sys.path.append('/home/storagetools')
from mods.common.vault.ver2.vault import Vault

vpath = 'it-storage/KVv1/netapp/common/napi_admin_user'
hashes = Vault(vpath).get_secret()


arg = args()
d ={'user': arg['<user>']}
num_workers = 12
vol_data = []
data_list = []
snap_data = []
table_data = []

def Headers():
        username = hashes['Data']['username']
        password = hashes['Data']['password']
        userpass = username + ':' + password
        encoded_u = base64.b64encode(userpass.encode()).decode()

        headers = {"Authorization" : "Basic %s" % encoded_u}

        return headers


def conv_time(time):
    """
    Convert snapshot creation_time from
      "create_time": "2019-02-04T19:00:00Z"
        to "create_time": Thu Sep 07 00:10:00 2023
    """
    parsed_datetime = parser.isoparse(time)

    # Format the datetime object into the desired format
    formatted_datetime_str = parsed_datetime.strftime("%a %b %d %H:%M:%S %Y")

    return formatted_datetime_str

def to_csv(fl, data):
    df = pd.DataFrame(data, columns=["vserver", "volume", "snapshot", "size", "reclaimable_space", "Qtree", "create-time"])
    df.to_csv(f"{fl}/{fl}.csv", index=False)

def to_json(fl,data):
    df = pd.DataFrame(data, columns=["vserver", "volume", "snapshot",  "size", "reclaimable_space","Qtree", "create-time"])
    df.to_json(f"{fl}/{fl}.json", lines=True, indent=2, orient='records')

def check_svm_state(storage,link):
    # Check if the svm state is running
    url = f"https://{storage}{link}"
    headers = Headers()

    try:
        resp = requests.get(url, verify=False, headers=headers)
        resp = resp.json()

        if resp['state'] == "running" or resp['state'] == "online":
            return True
        else:
            return False

    except Exception as err:
        print(f"Error in getting state!: {err}")


def get_svm(storage):
    url = f"https://{storage}/api/svm/svms"
    headers = Headers()

    try:
        # print(f"getting the data of the svm {storage}")
        resp = requests.get(url, verify=False, headers=headers)
        resp = resp.json()
        print(f"getting the data of the svms:")
        for i in resp["records"]:
            print(i['name'])

        logging.info(f"Getting all the SVM of the cluster {storage}", extra=d)
        logging.info(f"List of SVMs in the cluster: {', '.join(record['name'] for record in resp['records'])}", extra=d)


        for i in resp['records']:
            link = i['_links']['self']['href']
            if check_svm_state(storage, link):
                data_list.append(i['name'])

    except Exception as err:
        logging.info(f"Error getting data of the cluster {storage} \n ERROR!: {err}", extra=d)
        print(f"Error requests in svm!: {err}")

def get_vol(storage, svm):

    url = f"https://{storage}/api/storage/volumes?svm.name={svm}"
    headers = Headers()

    try:
        logging.info(f"Getting data from the svm {svm} ", extra=d)

        resp = requests.get(url, verify=False, headers=headers)
        resp = resp.json()

        for i in resp['records']:
            link = i['_links']['self']['href']
            if check_svm_state(storage, link):

                vol_data.append([svm, i['name'], i['uuid']])

    except Exception as err:
        logging.info(f"Error getting volumes from svm {svm} \n ERROR!: {err}", extra=d)

        print(f"Error requests in volume!: {err}")

def space_conv(size_bytes):
    if size_bytes == 0:
        return "0B"

    size_name = ("TB", "GB", "MB", "KB", "B")  # Add "B" for bytes
    i = int(np.floor(np.log2(size_bytes) / 10))

    if i >= len(size_name):
        i = len(size_name) - 1  # Use the last index if size is smaller than 1KB

    return f"{size_bytes / (2 ** (i * 10)): .2f}{size_name[i]}"


def get_snap_data(storage, link, qtree):
  """The function run a request that response the information within the given snapshot"""
  url = f"https://{storage}{link}"
  url_rec_space = f"https://{storage}{link}?fields=reclaimable_space"

  headers = Headers()

  try:
    resp = requests.get(url, verify=False, headers=headers)
    resp2 = requests.get(url_rec_space, verify=False, headers=headers)

    resp = resp.json()
    resp2 = resp2.json()

    #   convert time format from default to 'Wed Sep 06 22:05:00 2023'
    time = conv_time(str(resp['create_time']))
    size = space_conv(resp['size'])
    reclaimable_space = space_conv(resp2['reclaimable_space'])

    #  append snapsht info into the list(snap_data)
    snap_data.append([resp['svm']['name'], resp['volume']['name'], resp['name'], size, reclaimable_space,qtree, time])



  except Exception as err:
      print(f"Error in getting snapshot data!: {err}")

def get_snapshots(storage, uuid):
    url = f"https://{storage}/api/storage/volumes/{uuid}/snapshots"
    url_q3 = f"https://{storage}/api/storage/qtrees?volume.uuid={uuid}"

    headers = Headers()

    try:
        # logging.info(f"Getting the snapshots and qtrees from the svm {storage} and volume {uuid} ", extra=d)

        # Using ThreadPoolExecutor to run requests in parallel
        with ThreadPoolExecutor() as executor:
            # Submit requests and get the futures
            future_resp = executor.submit(requests.get, url, verify=False, headers=headers)
            future_resp_q3 = executor.submit(requests.get, url_q3, verify=False, headers=headers)

            # Wait for the responses
            resp = future_resp.result()
            resp_q3 = future_resp_q3.result()

        resp = resp.json()
        resp_q3 = resp_q3.json()
        if not any("snapmirror" in record['name'] for record in resp['records']):
            for i in resp['records']:
                link = i['_links']['self']['href']
                for e in resp_q3['records']:
                    get_snap_data(storage, link, e['name'])

    except Exception as err:
        logging.info(f"Error getting the snapshots and qtrees from the svm {storage} and volume {uuid} \n ERROR! : {err}", extra=d)
        print(f"Error in snapshot!: {err}")

def get_data(cluster):
    get_svm(cluster)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        executor.map(lambda x: get_vol(cluster, x), data_list)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        submit_func = lambda x: get_snapshots(cluster, x[2])
        executor.map(submit_func, vol_data)

    if snap_data:
        for row in snap_data:
            if '_root' not in row[1] and 'projects' not in row[1] and 'scratch' in row[1]:
                print(row)
                table_data.append(row)

        fl = f"{cluster}"
        directory = f'{fl}'
        os.makedirs(directory, exist_ok=True)

        to_csv(cluster, table_data)
        to_json(cluster, table_data)
        return table_data
