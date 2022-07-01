import asyncio
import json
from connectionController import *

#rc-tickets-poc.redis.cache.windows.net:6380,password=ggCSNd1d9EVbZ2Kipm07PxsdTgvoZMRVeAzCaPEMTIU=,ssl=True,abortConnect=False
r = getRedisClient()

async def on_event(partition_context, event):
    # Print the event data.
    JSONObject = json.loads(event.body_as_str(encoding='UTF-8'))
    tienda = JSONObject['Content']['Tienda']
    date = JSONObject['Content']["FechaAdm"]
    year = date[0:4]
    month = date[4:6]
    # Update the checkpoint so that the program doesn't read the events
    try:
        totalAno = f"totalAno{year}"
        totalAnoMes = f"totalAnoMes{year}-{month}"
        totalAnoTienda = f"totalAnoTienda{year}-{tienda}"
        totalAnoMesTienda = f"totalAnoMesTienda{year}-{month}-{tienda}"
        
        r.incr(totalAno)
        r.incr(totalAnoMes)
        r.incr(totalAnoTienda)
        r.incr(totalAnoMesTienda)
    except Exception as e:
        await partition_context.update_checkpoint(event)
        errortotalAno = f"errortotalAno-{year}"
        errortotalAnoMes = f"errortotalAnoMes-{year}-{month}"
        errortotalAnoTienda = f"errortotalAnoTienda-{year}-{tienda}"
        errortotalAnoMesTienda = f"errortotalAnoMesTienda-{year}-{month}-{tienda}"

        r.incr(errortotalAno)
        r.incr(errortotalAnoMes)
        r.incr(errortotalAnoTienda)
        r.incr(errortotalAnoMesTienda)

        print(e)
    else:
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