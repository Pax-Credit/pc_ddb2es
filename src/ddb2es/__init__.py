# https://github.com/bfansports/dynamodb-to-elasticsearch/blob/master/src/DynamoToES/index.py

import json
import re
import boto3
# import logging
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from dateutil.parser import parse

# The way to access app logger
# log = logging.getLogger('payin')

class DDBtoESHandler:
    '''
    Layer between DynamoDBEvent and ElasticSearch
    '''
    __reserved_fields = [ "uid", "_id", "_type", "_source", "_all", "_parent", "_fieldnames", "_routing", "_index", "_size", "_timestamp", "_ttl"]


    def __init__(self, elastic, logger):
        self.elastic = elastic
        self.logger = logger
        

    def event_process(self, event):
        for record in event:
            if record.event_name == "REMOVE": 
                # self.logger.debug("record.old_image: %s", record.old_image)
                image = record.old_image 
            else:
                # self.logger.debug("record.new_image: %s", record.new_image)
                image = record.new_image

            record_dict = self.__ddbrecord_to_json(image)
            # self.logger.debug("record.new_image.unmarshal: %s", record_dict)

            id = record_dict['pk']

            try:

                if record.event_name == "INSERT":
                  self.elastic.doc_insert(id, record_dict)
                elif record.event_name == "MODIFY":
                  self.elastic.doc_modify(id, record_dict)
                elif record.event_name == "REMOVE":
                  self.elastic.doc_remove(id)

            except Exception as e:
                self.logger.error(f"Failed for document ID: {id}. Raised: {repr(e)}")
                continue
            else:
                self.logger.debug(f"{record.event_name} success - Document ID: {id}")


    def __ddbrecord_to_json(self, record):
        data = {}
        data["M"] = record
        return self.__unmarshalValue(data, True)


    def __unmarshalValue(self, node, forceNum=False):
        '''
        recursive function
        '''
        for key, value in list(node.items()):
            if (key == "NULL"):
                return None
            if (key == "S" or key == "BOOL"):
                return value
            if (key == "N"):
                if (forceNum):
                    return self.__int_or_float(value)
                return value
            if (key == "M"):
                data = {}
                for key1, value1 in list(value.items()):
                    if key1 in self.__reserved_fields:
                        key1 = key1.replace("_", "__", 1)
                    data[key1] = self.__unmarshalValue(value1, True)
                return data
            if (key == "BS" or key == "L"):
                data = []
                for item in value:
                    data.append(self.__unmarshalValue(item))
                return data
            if (key == "SS"):
                data = []
                for item in value:
                    data.append(item)
                return data
            if (key == "NS"):
                data = []
                for item in value:
                    if (forceNum):
                        data.append(self.__int_or_float(item))
                    else:
                        data.append(item)
                return data


    # Detect number type and return the correct one
    def __int_or_float(self, s):
        try:
            return int(s)
        except ValueError:
            return float(s)



class ESHandler:
    '''
    Handles all elasticserch operations
    '''
    def __init__(self, endpoint, index_name, index_properties, logger):
        self.endpoint = endpoint
        self.index_name = index_name
        self.index_properties = index_properties
        self.logger = logger
        self.__validation_errors = []


    def index_exist(self):
        return self.connection.indices.exists(self.index_name) == True


    def index_create(self):
        body = {
            "mappings": {
                "properties": self.index_properties
            }
        }
        self.connection.indices.create(index=self.index_name, body=body)


    def index_delete(self):
        return self.connection.indices.delete(self.index_name)


    def doc_insert(self, id, data):
        self.__validation_errors = []
        
        if not self.__index_data_validate(data):
            raise ValueError(f'data not valid: {self.__validation_errors[0]}' )

        self.connection.index(index=self.index_name, body=data, id=id, refresh=True) 

        # self.logger.debug(f"Successly inserted - Document ID: {id}")


    def doc_modify(self, id, data):
        self.__validation_errors = []

        if not self.__index_data_validate(data):
            raise ValueError(f'data not valid: {self.__validation_errors[0]}' )

        # We reindex the whole document as ES accepts partial docs
        self.connection.index(index=self.index_name, body=data, id=id, refresh=True) 

        # self.logger.debug(f"Successly modified - Document ID: {id}")

    
    def doc_remove(self, id):
        self.connection.delete(index=self.index_name, id=id, refresh=True)

        # self.logger.debug(f"Successly removed - Document ID: {id}")


    def search(self, filters = {}, sortings = [], ranges = []):
        self.__validation_errors = []

        filters = self.__filters_validate(filters)
        sortings = self.__sortings_validate(sortings)
        ranges = self.__range_validate(ranges)

        body = {
            "query": {
                "match_all": {}
            },
            "sort" : [
                {"created_at" : "desc"}
            ],
            "from": int(ranges[0]),
            "size": int(ranges[1]),
        }


        if filters:
            filters_list = []
            for f in filters:
                filters_list.append({"term":{f:filters[f]}})
            
            body['query'] = {
                "bool": {
                    "must": filters_list,
                    # "must": [{"match_all": {}}]
                }
            }
            # body['query'] = {'match_phrase': filters}

        if sortings:
            body['sort'] = [sortings]

        # print('--- body =', body)
        
        results = self.connection.search(index=self.index_name, body=body)

        return results


    def __add_validation_error(self, record):
        self.__validation_errors.append(record)


    def get_validation_errors(self):
        return self.__validation_errors


    def __filters_validate(self, data):
        return self.__index_data_validate(data)
    

    def __sortings_validate(self, sortings):
        if type(sortings) != list or len(sortings) != 2:
            self.__add_validation_error(f'Sorting {sortings} have to be a list with 2 elements, setting defaults')
            sortings = ['created_at', 'desc']

        key = sortings[0]
        direction = sortings[1]

        if not key in self.index_properties:
            self.__add_validation_error(f'Unknown sortings key {key}')
            key = 'created_at'

        direction = 'asc' if direction.lower() == 'asc' else 'desc'

        return {key:direction}


    def __range_validate(self, range=[]):
        try:
            start = int(range[0])
        except:
            self.__add_validation_error(f'Range start is not integer, set default')
            start = 0

        try:
            limit = int(range[1])
        except:
            self.__add_validation_error(f'Range limit is not integer, set default')
            limit = 2

        return [start, limit]


    def __index_data_validate(self, data):
        result = {}
        for key in data:
            value = data[key]

            if not key in self.index_properties:
                self.__add_validation_error(f'Unknown data key {key}')
                continue

            if not value:
                continue

            typ = self.index_properties[key]['type']

            if typ == 'date':
                try: 
                    value = parse(value, fuzzy=False)
                except ValueError:
                    self.__add_validation_error(f'Data for key {key} is not a date: {value}')
                    continue
            elif typ == 'float':
                try:
                    value = float(value)
                except ValueError:
                    self.__add_validation_error(f'Data for key {key} is not a int/float: {value}')
                    continue
            elif typ == 'keyword' or typ == 'text':
                pass
            else:
                self.__add_validation_error(f'Unknown type for key {key}')
                continue

            result[key] = value

        return result


    def __getattr__(self, name):
        if name != 'connection':
            return

        if not self.__connection:
            session = boto3.session.Session()
            credentials = session.get_credentials()
            
            awsauth = AWS4Auth(credentials.access_key,
                               credentials.secret_key,
                               session.region_name, 'es',
                               session_token=credentials.token)    

            self.__connection = Elasticsearch(
                self.endpoint,
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection
            )
        return self.__connection