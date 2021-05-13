import json

import requests
from elasticsearch import Elasticsearch


class DataProcessor:
    def __init__(self, url):
        """
        Initialization
        :param url: url to send data from to Elasticsearch
        """
        self.url = url
        self.raw_data = None
        self.parsed_data = None
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def get_data(self):
        """
        It loads data from URL to raw_data
        """
        r = requests.get('http://api.tvmaze.com/schedule?country=UA&date=2021-05-13')
        self.raw_data = json.loads(r.content)

    def parse_data(self):
        """
        It gets data needed
        """
        result = []
        for row in self.raw_data[:20]:
            item = dict()
            item['id'] = row['id']
            item['season'] = row['season']
            item['number'] = row['number']
            item['airdate'] = row['airdate']
            item['name'] = row['show']['name']
            item['genres'] = row['show']['genres']
            item['network'] = row['show']['network']['name']
            result.append(item)
        self.parsed_data = result

    def create_index_in_es(self):
        """
        It sends data to Elasticsearch
        """
        self.es.indices.delete(index='tvshows', ignore=[400, 404])

        for doc in self.parsed_data:
            self.es.index(index='tvshows', doc_type='ukraine_shows', id=doc["id"], body=json.dumps(doc))

    def show_indexes_in_es(self):
        """
        It shows the existing indexes in ElasticSearch
        """
        r = requests.get("http://localhost:9200/_cat/indices?v")
        print(r.content)

    def show_types_in_es(self):
        """
        It shows the existing types for the index
        """
        r = requests.get("http://localhost:9200/tvshows/_mapping")
        json_result = json.loads(r.content)
        print(json.dumps(json_result, indent=4, sort_keys=True))

    def show_all(self):
        print("All documents")
        r = requests.get("http://localhost:9200/tvshows/_search?pretty")
        json_result = json.loads(r.content)
        print(json.dumps(json_result, indent=4, sort_keys=True))

    def get_by_id_es(self):
        print("Search by ID")
        doc = self.es.get(index='tvshows', id=2067859)
        print(doc)

    def custom_full_text_search(self):
        print("Full text search")
        docs = self.es.search(index="tvshows",
                              body={"query": {"wildcard": {'name': 'Хрустальные'}}})
        print(docs)

    def custom_fuzzy_full_text_search(self):
        print("Full text fuzzy search")
        docs = self.es.search(index="tvshows",
                              body={"query": {"fuzzy": {'name': 'случай'}}})
        print(docs)

    def close(self):
        self.es.close()

    def run(self):
        """
        It is to run the entire process
        """
        self.get_data()
        self.parse_data()
        # self.create_index_in_es()
        self.show_indexes_in_es()
        self.show_types_in_es()
        self.show_all()
        self.get_by_id_es()
        self.custom_full_text_search()
        self.custom_fuzzy_full_text_search()
        self.close()


data_processor = DataProcessor("http://api.tvmaze.com/schedule?country=UA&date=2021-05-13")
data_processor.run()
