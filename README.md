Blocked to use in requirements by: https://github.com/aws/chalice/issues/1516

# Dynamodb to Elasticsearch

This is a reusable solution to implement Elasticsearch as a filtering, sorting and paginating tool for Dynamodb data in specified table. Applied in AWS chalice framework

DDBtoESHandler based on this solution: 
https://github.com/bfansports/dynamodb-to-elasticsearch/blob/master/src/DynamoToES/index.py

## Installing

`pip install -e git+https://github.com/Pax-Credit/pc_ddb2es#egg=ddb2es`

For developers, To install in Development mode use -e:

`pip install -e /Users/anufry/www/sloboda/paxcredit/pc_ddb2es`

## Usage

### ESHandler

```python
from ddb2es import ESHandler, DDBtoESHandler

elastic = ESHandler(
  endpoint=os.environ["ES_ENDPOINT_URL"],
  index_name = os.environ["ES_INDEX_NAME"],
  index_properties = {
    'owner': {"type": "keyword"},
    'created_at': {"type": "date"},
    ...
    'address': {"type": "keyword"},
  },
  logger=app.log)
```

endpoint - is an url to Elasticsearch endpoint
index_name - is an index name for elasticsearch (usualy Tablename in lowercase)
index_properties - Elasticsearch index properties

Elasticsearch index operations

```python
elastic.index_exist()
elastic.index_create():
elastic.index_delete():
```

Working with indexed documents

```python
elastic.doc_insert(id, data):
elastic.doc_modify(id, data):
elastic.doc_remove(id):
```

Searching in documents

```python
elastic.search(filters = {}, sortings = [], ranges = []):
```

Getting validation errors for documents index & search operations

```python
elastic.get_validation_errors()
```

### DDBtoESHandler

```python
elastic = ESHandler(...)

ddbr = DDBtoESHandler(elastic=elastic, logger=app.log)
ddb_stream_arn=ddbr.get_ddb_stream_arn(os.environ["DDB_TABLE"])

@app.on_dynamodb_record(stream_arn=ddb_stream_arn, batch_size=100, starting_position='TRIM_HORIZON')
def elastic_stream_handler(event):
    ddbr.event_process(event)
```
