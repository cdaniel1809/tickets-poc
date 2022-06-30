import asyncio
from pickle import TRUE
from turtle import st
from ticketRaw import ticketRaw
import json
import time
from random import seed
from random import randint
from random import random
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import sys

with open('ticketRaw.json', 'r') as file:
    JSONdata = file.read().replace('\n', '')

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://eh-tickets-poc.servicebus.windows.net/;SharedAccessKeyName=lectura-escritura;SharedAccessKey=f4A1J5Yo+wvcHLGU7Wbp/JMzl3yoAcmx1QrSAwYCp2w=;EntityPath=recepcion-tickets", eventhub_name="recepcion-tickets")
    async with producer:
        # Create a batch.
        
        i = int(sys.argv[1])
        max = int(sys.argv[2])
        # Add events to the batch.
        while i <= max:
            for y in range(100):
                event_data_batch = await producer.create_batch()
                for x in range(222):
                    JSON = GetJSON(i)
                    event_data_batch.add(EventData(JSON))
                # Send the batch of events to the event hub.
                await producer.send_batch(event_data_batch)
                print (f"Tickets Sended Tienda {str(i)} enviados {str( (y + 1) * 222)} : total enviado  {str( ((i- 1) * 22000) + ((y + 1) * 222))}")
                #time.sleep(1)
            i += 1
            

def GetJSON(idtienda):
    year = "2022"
    month = "06" #randint(1, 12)
    day = randint(1, 30)
    storeid = f"Tienda{str(idtienda)}"  #str(randint(1,15000))
    folio = str(randint(1,1000000000))
    id = f"{year}{month}{day}-{storeid}-{folio}"
    expected = JSONdata.replace("||tienda||",storeid).replace("||fecha||",f"{year}{month}{day}").replace("||_id_||",id).replace("||_folio_||",folio)
    return expected

loop = asyncio.get_event_loop()
loop.run_until_complete(run())