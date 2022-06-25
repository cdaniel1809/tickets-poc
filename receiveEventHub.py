from calendar import month
import pymongo
import json
import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from ticketRaw import ticketRaw

connections = {}
inserted = 0

def get_Connection(year, month, collectionName, dbName):
    #key = f"{year}-{month}-{collectionName}"
    
    #if key in connections:
    #    return connections[key]
    #else:
    mongoConnectionString = f"mongodb://mongo-{year}-{month}.centralus.cloudapp.azure.com:27017/"
    myclient = pymongo.MongoClient(mongoConnectionString)
    mydb = myclient[dbName]
    mycol = mydb[collectionName]
    #connections[key] = mycol
    return mycol #connections[key]

def getInserted():
    global inserted
    return inserted

def increaseInserted():
    global inserted
    inserted += 1

def clearInserted():
    global inserted
    inserted = 0

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
    except:
        print("Error")
    else:
        increaseInserted()
        if getInserted() % 10 == 0:
            print (f"Saving rows {getInserted()}in DB : {db}")
        #    clearInserted = 0

        #print(f" the object has been add to the list : {count}")
        # that it has already read when you run it next time.
        await partition_context.update_checkpoint(event)

async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string("DefaultEndpointsProtocol=https;AccountName=ehcheckpointoxxopoc;AccountKey=HlmWqKXfJfRWlrdVRbFf/kPhB/smAP0nlWjyeZS0vVoENPmZ0KYjS3g45QQwQ0Dtz+BqplKrlOSB+AStC23NiQ==;EndpointSuffix=core.windows.net", "checkpoint")

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string("Endpoint=sb://eh-oxxo-poc.servicebus.windows.net/;SharedAccessKeyName=read-write;SharedAccessKey=K6fJKPN59x3PK+hqT5NcdzuHi7AETeC1B7SdT30tUDg=;EntityPath=recepcion-tickets", consumer_group="$Default", eventhub_name="recepcion-tickets", checkpoint_store=checkpoint_store)
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())