import json
import random
import requests
from time import sleep
from db_utils import DBUtils

random.seed(100)

db_utils = DBUtils()

def run_infinite_streaming_data_loop():
    '''
    This function calls upon running of this file, and serves to emulate users posting to Pinterest, 
        via streaming data format.
    The data streamed consists of three different datasets: pin, user, and geo. 
    Each dataset is extracted using the db_utils method, 
        then passed into a JSON serializer to convert it to the correct structure,
        to then be streamed to an API endpoint.
    '''
    
    while True:
        sleep(random.randrange(0, 2))
        headers = {'Content-Type': 'application/json'}
        # The URL of the endpoint set up in AWS API Gateway
        
        # pin dataset 
        pin_result = db_utils.extract_data('pinterest_data')
        pin_payload = json.dumps({
            "StreamName": "streaming-0e2b04098249-pin",
            "Data": {
                'index': pin_result['index'],
                'unique_id': pin_result['unique_id'],
                'title': pin_result['title'],
                'description': pin_result['description'],
                'poster_name': pin_result['poster_name'],
                'follower_count': pin_result['follower_count'],
                'tag_list': pin_result['tag_list'],
                'is_image_or_video': pin_result['is_image_or_video'],
                'image_src': pin_result['image_src'],
                'downloaded': pin_result['downloaded'],
                'save_location': pin_result['save_location'],
                'category': pin_result['category']
                },
            "PartitionKey": "desired-name"
            })
        
        topics_list = ['streaming-0a1667ad2f7f-pin', 'streaming-0a1667ad2f7f-geo', 'streaming-0a1667ad2f7f-user']
        # invoke_url = f'https://rfrcndbjtb.execute-api.us-east-1.amazonaws.com/prod/streams/{topics_list}/record'
        invoke_url = f'https://c9joj9e3ij.execute-api.us-east-1.amazonaws.com/test/streams/streaming-0a54b96ac143-pin/record'

        pin_response = requests.request(
            "PUT", 
            invoke_url,
            headers=headers, 
            data=pin_payload
            )
        print(pin_payload)
        print('pin_response: ' + str(pin_response.status_code) + str(pin_response.text))

        # geo dataset
        # geo_result = db_utils.extract_data('geolocation_data')
        # geo_payload = json.dumps({
        #     "StreamName": "streaming-0a2a5872851b-geo",
        #     "Data": {
        #         'ind': geo_result['ind'],
        #         'timestamp': str(geo_result['timestamp']),
        #         'latitude': geo_result['latitude'],
        #         'longitude': geo_result['longitude'],
        #         'country': geo_result['country']
        #         },
        #     "PartitionKey": "desired-name"
        #     })
    
        # geo_response = requests.request(
        #     "PUT",
        #     invoke_url.format('streaming-0a2a5872851b-geo'),
        #     headers=headers,
        #     data=geo_payload
        #     )
        # print('geo_response: ' + str(geo_response.status_code) + str(pin_response.text))

        # user dataset
        # user_result = db_utils.extract_data('user_data')
        # user_payload = json.dumps({
        #     "StreamName": "streaming-0a2a5872851b-user",
        #     "Data": {
        #         'ind': user_result['ind'],
        #         'first_name': user_result['first_name'],
        #         'last_name': user_result['last_name'],
        #         'age': user_result['age'],
        #         'date_joined': str(user_result['date_joined'])
        #         },
        #     "PartitionKey": "desired-name"
        #     })
    
        # user_response = requests.request(
        #     "PUT",
        #     invoke_url.format('streaming-0a2a5872851b-user'),
        #     headers=headers,
        #     data=user_payload
        #     )
        # print('user_response: ' + str(user_response.status_code) + str(pin_response.text))

if __name__ == "__main__":
    run_infinite_streaming_data_loop()
    
    


