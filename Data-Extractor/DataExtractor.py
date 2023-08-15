import json
import urllib.request
import pymongo


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

    def insert_data(self, documents):
        self.collection.insert_many(documents)

    def print_collection(self):
        print(self.collection.count_documents({}))
        for doc in self.collection.find().limit(5):
            print(doc)


def main():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    # url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=dc615ff7-2ff3-416a-922b-f0f334f085d0'
    url = 'https://data.boston.gov/api/3/action/package_show?id=rentsmart'
    mongo_uri = config["mongo_uri"]
    db_name = config["db_name"]
    collection_name = config["collection_name"]
    extractor = DataExtractor(mongo_uri, db_name, collection_name)
    extractor.url = url  # Set the URL
    response_dict = extractor.fetch_data()
    print(response_dict['result']['resources'][0]['last_modified'])
    # documents = response_dict.get("result", [])  # Extract the list of documents
    # print(documents)
    # extractor.insert_data(documents)
    # extractor.print_collection()


if __name__ == "__main__":
    main()
