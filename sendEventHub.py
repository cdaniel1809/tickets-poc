import asyncio
from pickle import TRUE
from ticketRaw import ticketRaw
import json
import time
from random import seed
from random import randint
from random import random
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://eh-oxxo-poc.servicebus.windows.net/;SharedAccessKeyName=read-write;SharedAccessKey=K6fJKPN59x3PK+hqT5NcdzuHi7AETeC1B7SdT30tUDg=;EntityPath=recepcion-tickets", eventhub_name="recepcion-tickets")
    async with producer:
        # Create a batch.
        
        i = 1
        # Add events to the batch.
        while i < 3000000:
            event_data_batch = await producer.create_batch()
            for x in range(200):
                JSON = json.dumps(GetJSON().__dict__)
                event_data_batch.add(EventData(JSON))

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)

            i += 1

            print (f"Tickets Sended {str( i * 200)}")
            time.sleep(1)

def GetJSON():
    
    JSONObject = ticketRaw()
    JSONObject.year = "2022"
    JSONObject.month = "06" #randint(1, 12)
    JSONObject.storeid = "Tienda" + str(randint(1,15000))
    JSONObject.total = random() * randint(1,15000)

    return JSONObject

loop = asyncio.get_event_loop()
loop.run_until_complete(run())