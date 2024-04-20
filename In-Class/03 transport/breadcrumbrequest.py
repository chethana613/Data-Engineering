import requests
import json

def fetch_data():
    vehicle_ids = [3406, 3257]
    base_url = 'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id='
    data = {}
    for vehicle_id in vehicle_ids:
        url = base_url + str(vehicle_id)
        response = requests.get(url)
        if response.status_code == 200:
            data[f'vehicle_{vehicle_id}'] = response.json()
        else:
            print(f'Failed to fetch data for vehicle {vehicle_id}: {response.status_code}, {response.reason}')

    with open('bcsample.json', 'w') as file:
        json.dump(data, file, indent=4)

if __name__ == '__main__':
    fetch_data()