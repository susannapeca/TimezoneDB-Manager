import requests
from api_key import api_key

base_url = "http://api.timezonedb.com/v2.1/"

def get_time_zone_list():
    '''
    Obtains the response of the endpoint that lists all the time zones.
    :return: response of the time zone list endpoint
    '''
    params = {'key': api_key, 'format': 'json'}
    response = send_request(base_url + 'list-time-zone', params=params)
    return response

def get_time_zone_details(time_zone:str):
    '''
    Obtain the response of the endpoint that retrieves information about a specific time zone
    :param time_zone: the time zone
    :return: response of the details about the specific endpoint
    '''
    params = {'key': api_key, 'format': 'json', 'by': 'zone', 'zone': time_zone}
    response = send_request(base_url + 'get-time-zone', params=params)
    return response


def send_request(request_url, params=None):
    '''
    Handles sending the HTTP request to the time zone API
    :param request_url: request URL
    :param params: request parameters
    :return: endpoint response
    '''
    try:
        if params is None:
            params = {'key': api_key, 'format': 'json'}

        response = requests.get(request_url, params=params)
        return response

    except Exception as error:
        pass
