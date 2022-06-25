import asyncio
import redis
import json
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

#rc-tickets-poc.redis.cache.windows.net:6380,password=ggCSNd1d9EVbZ2Kipm07PxsdTgvoZMRVeAzCaPEMTIU=,ssl=True,abortConnect=False
r = redis.StrictRedis(host='rc-tickets-poc.redis.cache.windows.net',
        port=6379, db=0, password='ggCSNd1d9EVbZ2Kipm07PxsdTgvoZMRVeAzCaPEMTIU=', ssl=False)

async def on_event(partition_context, event):
    # Print the event data.
    JSONObject = json.loads(event.body_as_str(encoding='UTF-8'))

    # Update the checkpoint so that the program doesn't read the events
    
    key = f"total{JSONObject['year']}-{JSONObject['month']}-{JSONObject['storeid']}"
    r.incr(key)
    
  
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string("DefaultEndpointsProtocol=https;AccountName=ehcheckpointoxxopoc;AccountKey=HlmWqKXfJfRWlrdVRbFf/kPhB/smAP0nlWjyeZS0vVoENPmZ0KYjS3g45QQwQ0Dtz+BqplKrlOSB+AStC23NiQ==;EndpointSuffix=core.windows.net", "checkpoint")

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string("Endpoint=sb://eh-oxxo-poc.servicebus.windows.net/;SharedAccessKeyName=read-write;SharedAccessKey=K6fJKPN59x3PK+hqT5NcdzuHi7AETeC1B7SdT30tUDg=;EntityPath=recepcion-tickets", consumer_group="rediscounters", eventhub_name="recepcion-tickets", checkpoint_store=checkpoint_store)
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())