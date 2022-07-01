import json
import asyncio
from connectionController import *
from facturaGlobalXMLCreator import *
from azure.servicebus import ServiceBusClient, ServiceBusMessage

async def main():
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)
    while 1 == 1:
        with servicebus_client:
            receiver = servicebus_client.get_subscription_receiver(topic_name=TOPIC_NAME, subscription_name=SUBSCRIPTION_NAME, max_wait_time=5) 
            with receiver:
                for msg in receiver:
                    JSONObject =json.loads(str(msg))
                    print("Received: " + str(msg))
                    crearXML(JSONObject['ano'],JSONObject['mes'], f"Tienda{JSONObject['tienda']}")
                    receiver.complete_message(msg)
        time.sleep(1)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())
    #crearXML("2022","06", "Tienda1002")