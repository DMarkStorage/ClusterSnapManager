from util import *
# import json

__program__ = 'test_cluster'
__version__ = 'Version 1'
__revision__ = 'Initial program'


def args():

    usage = """
        Usage:
            get_cluster.py <datacenter> --datacenter [--opt=<action>] <user>
            get_cluster.py <datacenter> --cluster [--opt=<action>] <user>
            get_cluster.py --version
            get_cluster.py (-h | --help)


        Options:
            -h --help     Show this help message and exit.
            --opt=<action>     Specify the action to perform: 'show' or 'delete'.
        """
    version = '{} VER: {} REV: {}'.format(__program__, __version__, __revision__)
    args = docopt(usage, version=version)
    return args

def job_state(cluster, snap_name, link):

    url = f"https://{cluster}{link}"
    print(f"job url: {url}")
    headers = Headers()

    try:
        resp = requests.get(url, headers=headers, verify=False)
        resp = resp.json()

        if resp['state'] == 'failure':
            logging.info(f"Failed to delete snapshot {snap_name}! {resp['message'].upper()}", extra=d)
            print(f"Failed to delete snapshot {snap_name}! {resp['message'].upper()}")
        elif resp['state'] == 'success':
            logging.info(f"Snapshot {snap_name} DELETED!")
            print(f"Snapshot {snap_name} DELETED!")
        else:
            logging.info(f"Failed to delete snapshot {snap_name}! {resp['state']}: {resp['message']}", extra=d)
            print(f"{resp['state']}: {resp['message']}")



    except Exception as err:
        print(f"Error in getting snapshot data!: {err}")

def del_snap(cluster, snap_name, link):
  headers = Headers()

  try:
      resp = requests.delete(link, headers=headers, verify=False)

      resp = resp.json()
      url = resp['job']['_links']['self']['href']
      job_state(cluster, snap_name, url)

  except Exception as err:
      print(f"Error! {err}")

def clusters_data(json_response):
    if json_response and 'data' in json_response and 'result' in json_response['data']:
        clusters = json_response['data']['result']
        for cluster in clusters:
            cluster_name = cluster['metric']['cluster']
            logging.info(f"Getting data of the cluster {cluster_name}", extra=d)
            print(f"Getting data of the cluster {cluster_name}")
            data = get_data(cluster_name)
            if arguments['--opt'] == 'show':
                for i in data:
                    print(f"snapshot {i[2]} will be deleted : DELETE {i[7]}")
            elif arguments['--opt'] == 'delete':
                for i in data:
                    logging.info(f"Deleting snapshot {i[2]}", extra=d)
                    print(f"Deleting snapshot {i[2]}")
                    del_snap(cluster_name, i[2], i[7])




def query_api(params):
    url = 'http://storage-grafana-01:8082/api/v1/query'

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        json_data = response.json()
        logging.info(f"Getting all cluster from datacenter {arguments['<datacenter>']}", extra=d)
        print(f"Successfully data from cluster {arguments['<datacenter>']}")
        clusters_data(json_data)

    except requests.exceptions.RequestException as e:
        logging.info(f"Error getting pulling data from storage! ERROR! : {e}", extra=d)
        print("Error:", e)
        return None

def  main(args):
    datacenter = args['<datacenter>']

    if arguments['--datacenter']:
        logging.info(f"Getting all cluster from datacenter {datacenter}", extra=d)
        print(f"Getting all cluster from datacenter {datacenter}")
        params = {
            'query': 'cluster_status{datacenter="' + datacenter + '"}'
            }

        query_api(params)


    elif arguments['--cluster']:
        logging.info(f"Getting data from cluster {datacenter}", extra=d)
        print(f"Getting data from cluster {datacenter}")
        params = {
            'query': 'cluster_status{cluster="' + datacenter + '"}'
            }
        query_api(params)


if __name__ == '__main__':
    try:
      # Get args from docopt
        arguments = args()
        logging.info(f"User entered username: {arguments['<user>']}", extra=d)

        main(arguments)

    except Exception as err:
        print(f'Error! : {err}')
