from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.storage.blob import BlobClient
from datetime import datetime
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import redis
import pymongo

connections = {}

SUBSCRIPTION_NAME="facturaglobalprocesadores"
TOPIC_NAME = "facturaglobal"
CONNECTION_STR = "Endpoint=sb://sb-tickets-poc.servicebus.windows.net/;SharedAccessKeyName=readwrite;SharedAccessKey=0oe82nNd3wqyRdl1k0zTR5h/VrwxiC2qrlVAsc/lY7U=;EntityPath=facturaglobal"


def getBlobClient(ano, mes, tienda):
    strDate = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    client = BlobClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=ehcheckpointoxxopoc1;AccountKey=e4oFqUXTmjhxU56/zuPys78RfYeecbylPv6Mc6nc3z/Wf/F+XVZ5MFDmmZqtuIpSR7ZDkHgdMgSV+AStJagzGg==;EndpointSuffix=core.windows.net", container_name="facturasglobales", blob_name=f"{ano}/{mes}/{tienda}_{strDate}.xml")
    return client

def getEventHubClient(checkpointContainer, consumerGroup, eventHubName):
    checkpoint_store = BlobCheckpointStore.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName=ehcheckpointoxxopoc1;AccountKey=e4oFqUXTmjhxU56/zuPys78RfYeecbylPv6Mc6nc3z/Wf/F+XVZ5MFDmmZqtuIpSR7ZDkHgdMgSV+AStJagzGg==;EndpointSuffix=core.windows.net", f"{checkpointContainer}")
    client = EventHubConsumerClient.from_connection_string("Endpoint=sb://eh-tickets-poc.servicebus.windows.net/;SharedAccessKeyName=lectura-escritura;SharedAccessKey=f4A1J5Yo+wvcHLGU7Wbp/JMzl3yoAcmx1QrSAwYCp2w=;EntityPath=recepcion-tickets", consumer_group=f"{consumerGroup}", eventhub_name=f"{eventHubName}", checkpoint_store=checkpoint_store)
    return client

def getRedisClient():
    client = redis.StrictRedis(host='rc-tickets-poc-op.redis.cache.windows.net',
        port=6380, db=0, password='66hRm96lEwHxmMmaVwQjp8aelOQrPpdtQAzCaEPQdME=', ssl=True)
    return client

def getMongoControllerConnection():
    global connections
    key = f"ControllerDB"
    dbName = key
    collectionName = "FacturasGlobales"
    if key in connections:
        return connections[key]
    else:
        mongoConnectionString = f"mongodb://mongoAdmin:Pa$$w0rd#12345@mongo-2022-06.eastus.cloudapp.azure.com:27017/?authSource=admin&authMechanism=SCRAM-SHA-256"
        myclient = pymongo.MongoClient(mongoConnectionString)
        mydb = myclient[dbName]
        mycol = mydb[collectionName]
        connections[key] = mycol
        return connections[key]

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
