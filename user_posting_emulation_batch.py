
import json
import random
import requests
from time import sleep
from utils import PostingUtils

random.seed(100)

def run_infinite_batch_data_loop():
    '''
    This function calls upon running of this file, and serves to emulate users posting to Pinterest, 
        via batch data format.
    The data streamed consists of three different datasets: pin, user, and geo. 
    Each dataset is extracted using the extract_data method, 
        then passed into a JSON serializer to convert it to the correct structure,
        to then be streamed to an API endpoint.
    '''
    posting_utils = PostingUtils()

    invoke_url = 'https://0q97clwsr9.execute-api.us-east-1.amazonaws.com/dev/topics/{}'

    while True:
        sleep(random.randrange(0, 2))

        # pin dataset
        pin_payload = posting_utils.payload_generator('pinterest_data')
        pin_response = posting_utils.data_send('0a2a5872851b.pin', pin_payload, invoke_url)
        print(pin_response)

        # geo dataset
        geo_payload = posting_utils.payload_generator('geolocation_data')
        geo_response = posting_utils.data_send('0a2a5872851b.geo', geo_payload, invoke_url)
        print(geo_response)

        # user dataset
        user_payload = posting_utils.payload_generator('user_data')
        user_response = posting_utils.data_send('0a2a5872851.user', user_payload, invoke_url)
        print(user_response)

if __name__ == "__main__":
    run_infinite_batch_data_loop()

    
    


