import pymongo
import json
import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from ticketRaw import ticketRaw

connections = {}
inserted = 0

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

async def on_event(partition_context, event):
    # Print the event data.
    
    #print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))
    JSONObject = json.loads(event.body_as_str(encoding='UTF-8'))

    # Update the checkpoint so that the program doesn't read the events
    
    db = "pruebasConexion"
    collectionName = f"ticketsRaw{JSONObject['Content']['Tienda']}"
    date = JSONObject['Content']["FechaAdm"]
    year = date[0:4]
    month = date[4:6]
    mycol = get_Connection(year, month,collectionName,db)
    
    try:
        x = mycol.insert_one(JSONObject)
    except Exception as e:
        await partition_context.update_checkpoint(event)
        print(e)
    else:
        # that it has already read when you run it next time.
        await partition_context.update_checkpoint(event)

async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string("DefaultEndpointsProtocol=https;AccountName=ehcheckpointoxxopoc1;AccountKey=e4oFqUXTmjhxU56/zuPys78RfYeecbylPv6Mc6nc3z/Wf/F+XVZ5MFDmmZqtuIpSR7ZDkHgdMgSV+AStJagzGg==;EndpointSuffix=core.windows.net", "checkpoint")

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string("Endpoint=sb://eh-tickets-poc.servicebus.windows.net/;SharedAccessKeyName=lectura-escritura;SharedAccessKey=f4A1J5Yo+wvcHLGU7Wbp/JMzl3yoAcmx1QrSAwYCp2w=;EntityPath=recepcion-tickets", consumer_group="ticketsraw", eventhub_name="recepcion-tickets", checkpoint_store=checkpoint_store)
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())