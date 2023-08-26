import json
import logging
import os
import urllib.request
from datetime import datetime

import boto3
import pymongo
# import pytz
import pendulum
from botocore.exceptions import ClientError


class DataExtractor:
    def __init__(self, mongo_uri, db_name, collection_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def fetch_data(self):
        if self.url is None:
            raise ValueError("URL is not set")

        fileobj = urllib.request.urlopen(self.url)
        response_dict = json.loads(fileobj.read())
        return response_dict

    def upload_to_s3(self, file_name, bucket, s3, object_name=None):
        if object_name is None:
            object_name = file_name

        s3_client = s3
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def save_temporary_csv(self, file_name, data):
        with open(file_name, 'wb') as csv_file:
            csv_file.write(data)


def main():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    # Setting parameters
    url = 'https://data.boston.gov/api/3/action/package_show?id=rentsmart'
    mongo_uri = config["mongo_uri"]
    db_name = config["db_name"]
    collection_name = config["collection_name"]

    # AWS Credentials
    session = boto3.Session(profile_name='default')
    s3 = session.client('s3')
    bucket_name = 'rentsmart-inbound'

    # Create an instance of the dataExtractor class
    extractor = DataExtractor(mongo_uri, db_name, collection_name)
    extractor.url = url  # Set the URL
    response_dict = extractor.fetch_data()
    last_modified_time_string = response_dict['result']['resources'][0]['last_modified']
    # est_tz = pytz.timezone('US/Eastern')
    # last_modified_time = est_tz.localize(datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S.%f"))
    last_modified_time = pendulum.parse(last_modified_time_string, tz='US/Eastern')
    current_time_est = pendulum.now('US/Eastern')
    # Calculate the time difference
    # time_difference = current_time_est - last_modified_time
    time_difference = current_time_est.diff(last_modified_time).in_hours()
    # Check if the time difference is less than 24 hours
    if time_difference < 24:
        print("The time difference is less than 24 hours.")
        # Get the data source URL
        data_source_url = response_dict['result']['resources'][0]['url']
        # Get the data source CSV
        data_source_csv = urllib.request.urlopen(data_source_url).read()

        # Save the data temporarily as a CSV file
        temp_file_name = "temporary_data.csv"
        extractor.save_temporary_csv(temp_file_name, data_source_csv)

        # Upload the temporary CSV file to S3 with date stamp
        s3_object_name = f"data_{current_time_est.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        if extractor.upload_to_s3(temp_file_name, bucket_name, s3, object_name=s3_object_name):
            print("The file was uploaded successfully.")

        # Delete the temporary CSV file
        os.remove(temp_file_name)

    else:
        print("The time difference is more than 24 hours: ", time_difference.total_seconds() / 60 / 60, "hours")


if __name__ == "__main__":
    main()
