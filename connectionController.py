from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
import redis
import pymongo

connections = {}

def getEventHubClient():
    checkpoint_store = BlobCheckpointStore.from_connection_string("DefaultEndpointsProtocol=https;AccountName=ehcheckpointoxxopoc1;AccountKey=e4oFqUXTmjhxU56/zuPys78RfYeecbylPv6Mc6nc3z/Wf/F+XVZ5MFDmmZqtuIpSR7ZDkHgdMgSV+AStJagzGg==;EndpointSuffix=core.windows.net", "checkpoint-redis")
    client = EventHubConsumerClient.from_connection_string("Endpoint=sb://eh-tickets-poc.servicebus.windows.net/;SharedAccessKeyName=lectura-escritura;SharedAccessKey=f4A1J5Yo+wvcHLGU7Wbp/JMzl3yoAcmx1QrSAwYCp2w=;EntityPath=recepcion-tickets", consumer_group="rediscounters", eventhub_name="recepcion-tickets", checkpoint_store=checkpoint_store)
    return client

def getRedisClient():
    client = redis.StrictRedis(host='rc-tickets-poc-op.redis.cache.windows.net',
        port=6380, db=0, password='66hRm96lEwHxmMmaVwQjp8aelOQrPpdtQAzCaEPQdME=', ssl=True)
    return client

def get_Connection(year, month, collectionName, dbName):
    global connections
    key = f"{year}-{month}"
    if key in connections:
        return connections[key]
    else:
        mongoConnectionString = f"mongodb://mongoAdmin:Pa$$w0rd#12345@mongo-{year}-{month}.eastus.cloudapp.azure.com:27017/?authSource=admin&authMechanism=SCRAM-SHA-256"
        myclient = pymongo.MongoClient(mongoConnectionString)
        mydb = myclient[dbName]
        mycol = mydb[collectionName]
        connections[key] = mycol
        return connections[key]
