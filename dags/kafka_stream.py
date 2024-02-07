import json


def get_data():
    """
    Get data from random-user API
    """

    import requests

    response = requests.get('https://randomuser.me/api/')
    response = response.json()
    response = response['results'][0]

    return response


def format_data(response):
    data = {}

    data['id'] = response['login']['uuid']
    data['gender'] = response['gender']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']

    location = response['location']
    data['address'] = (f"{str(location['street']['number'])} {location['street']['name']}"
                       f"{location['city']} {location['state']} {location['country']}")
    data['postcode'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data


def stream_data():
    response = get_data()
    data = format_data(response)
    data = json.dumps(data, ensure_ascii=False).encode('utf-8').decode()
