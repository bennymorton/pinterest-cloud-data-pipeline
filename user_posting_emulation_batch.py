import json
import random
import requests
from time import sleep
from db_utils import DBUtils

random.seed(100)

class UserPostingEmulationBatch:
    '''
    The methods in this class serve to emulate the process of a user posting to Pinterest, generating data in three different datasets.
    '''
    def __init__(self) -> None:
        self.headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        # URL for API endpoint set up in AWS API Gateway
        self.invoke_url = 'https://0q97clwsr9.execute-api.us-east-1.amazonaws.com/dev/topics/0a2a5872851b.pin'
        self.mahdi_url = "https://4gzrfhhikb.execute-api.us-east-1.amazonaws.com/test/topics/0af64aa61d45.pin"
        self.pilar_url = 'https://39aze7kl52.execute-api.us-east-1.amazonaws.com/test/topics/0ad4bf933ef7.pin'
        self.grace_url = 'https://amdpzoul4j.execute-api.us-east-1.amazonaws.com/test/topics/12a3da8f7ced.pin'
        self.christian_url = 'https://iijg6a7epl.execute-api.us-east-1.amazonaws.com/Development/topics/0e2bc66a6297.pin'
        self.saqub_url = 'https://m1c8pv5ln1.execute-api.us-east-1.amazonaws.com/test/topics/0aa58e5ad07d.pin'
        self.adam_url = 'https://jl1y5b7yai.execute-api.us-east-1.amazonaws.com/dev/topics/0e95b18877fd.pin'
        self.db_utils = DBUtils()
    
    def data_send(self, payload):
        response = requests.request(
            "POST",
            self.adam_url,
            headers=self.headers,
            data=payload
            )
        response_full = ('pin_response:' + str(response.status_code) + str(response.text))
        return response_full

    def pin_data_payload_generator(self):
        result = self.db_utils.extract_data('pinterest_data')
        payload = json.dumps(
                {"records": [{
                    "value": {
                        'index': result['index'],
                        'unique_id': result['unique_id'],
                        'title': result['title'],
                        'description': result['description'],
                        'poster_name': result['poster_name'],
                        'follower_count': result['follower_count'],
                        'tag_list': result['tag_list'],
                        'is_image_or_video': result['is_image_or_video'],
                        'image_src': result['image_src'],
                        'downloaded': result['downloaded'],
                        'save_location': result['save_location'],
                        'category': result['category']
                        }}]
                    }    
                )
        return payload

    def geo_data_payload_generator(self):
        result = self.db_utils.extract_data('geolocation_data')
        payload = json.dumps(
                {"records": [{
                    "value": {
                        'ind': result['ind'],
                        'timestamp': str(result['timestamp']),
                        'latitude': result['latitude'],
                        'longitude': result['longitude'],
                        'country': result['country']
                        }}]
                    }
                )
        return payload

    def user_data_payload_generator(self):
        result = self.db_utils.extract_data('user_data')
        payload = json.dumps(
                {"records": [{
                    "value": {
                        'ind': result['ind'],
                        'first_name': result['first_name'],
                        'last_name': result['last_name'],
                        'age': result['age'],
                        'date_joined': str(result['date_joined'])
                        }}]
                    }
                )
        return payload

    def run_infinite_batch_data_loop(self):
        '''
        This function calls upon running of this file, and serves to emulate users posting to Pinterest, 
            via batch data format.
        The data streamed consists of three different datasets: pin, user, and geo. 
        Each dataset is extracted using the extract_data method, 
            then passed into a JSON serializer to convert it to the correct structure,
            to then be streamed to an API endpoint.
        '''

        while True:
            sleep(random.randrange(0, 2))

            # pin dataset
            pin_payload = self.pin_data_payload_generator()
            pin_response = self.data_send(pin_payload)
            # print(pin_payload)
            print(pin_response)

            # geo dataset
            # geo_payload = self.geo_data_payload_generator()
            # geo_response = self.data_send('0a2a5872851b.geo', geo_payload)
            # print(geo_response)

            # # user dataset
            # user_payload = self.user_data_payload_generator()
            # user_response = self.data_send('0a2a5872851.user', user_payload)
            # print(user_response)

if __name__ == "__main__":
    user_posting_emulation_batch = UserPostingEmulationBatch()
    user_posting_emulation_batch.run_infinite_batch_data_loop()

    
    


