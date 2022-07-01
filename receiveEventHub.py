from calendar import month
import json
import asyncio
from ticketRaw import ticketRaw
from connectionController import *


r = getRedisClient()

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

    totalAnoMongo = f"totalAnoMongo{year}"
    totalAnoMesMongo = f"totalAnoMesMongo{year}-{month}"
    totalAnoTiendaMongo = f"totalAnoTiendaMongo{year}-{collectionName}"
    totalAnoMesTiendaMongo = f"totalAnoMesTiendaMongo{year}-{month}-{collectionName}"
    
    errorTotalAnoMongo = f"errortotalAnoMongo{year}"
    errorAnoMesMongo = f"errortotalAnoMesMongo{year}-{month}"
    errorTotalAnoTiendaMongo = f"errortotalAnoTiendaMongo{year}-{collectionName}"
    errorTotalAnoMesTiendaMongo = f"errortotalAnoMesTiendaMongo{year}-{month}-{collectionName}"
    

    try:
        x = mycol.insert_one(JSONObject)

        r.incr(totalAnoMongo)
        r.incr(totalAnoMesMongo)
        r.incr(totalAnoTiendaMongo)
        r.incr(totalAnoMesTiendaMongo)

    except Exception as e:
        await partition_context.update_checkpoint(event)
        r.incr(errorTotalAnoMongo)
        r.incr(errorAnoMesMongo)
        r.incr(errorTotalAnoTiendaMongo)
        r.incr(errorTotalAnoMesTiendaMongo)

        print(e)
    else:
        
        #print(f" the object has been add to the list : {count}")
        # that it has already read when you run it next time.
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