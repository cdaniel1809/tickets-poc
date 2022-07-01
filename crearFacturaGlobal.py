import json
import asyncio
from connectionController import *

async def on_event(partition_context, event):
    # Print the event data.
    
    #print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))
    #JSONObject = json.loads(event.body_as_str(encoding='UTF-8'))

    # Update the checkpoint so that the program doesn't read the events
    
    #db = "pruebasConexion"
    #collectionName = f"ticketsRaw{JSONObject['Content']['Tienda']}"
    #date = JSONObject['Content']["FechaAdm"]
    #year = date[0:4]
    #month = date[4:6]
    #mycol = get_Connection(year, month,collectionName,db)
    
    
    
    await partition_context.update_checkpoint(event)

async def main():
    client = getEventHubClient()
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())