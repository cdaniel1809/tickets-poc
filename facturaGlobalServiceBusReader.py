import json
import asyncio
from connectionController import *
from facturaGlobalXMLCreator import *
from azure.servicebus import ServiceBusClient
from applicationinsights import TelemetryClient

logging = TelemetryClient(APPINSIGHT_INSTRUMENTATION_HEY)


async def main():
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)
    while 1 == 1:
        with servicebus_client:
            receiver = servicebus_client.get_subscription_receiver(topic_name=TOPIC_NAME, subscription_name=SUBSCRIPTION_NAME, max_wait_time=5) 
            with receiver:
                for msg in receiver:
                    try:
                        JSONObject =json.loads(str(msg))
                        ano = JSONObject['ano']
                        mes = JSONObject['mes']
                        tienda = f"Tienda{JSONObject['tienda']}"
                        
                        logging.track_event("Received: " + str(msg))
                        receiver.complete_message(msg)
                        logging.track_event(f"Inicia la generacion de XML : ano : {ano}, mes : {mes}, tienda : {tienda} ")
                        logging.flush()

                        crearXML(ano,mes,tienda )
                        
                        logging.track_event(f"Se ha generado el XML : ano : {ano}, mes : {mes}, tienda : {tienda} ")
                        logging.flush()
                        
                    except Exception as e:
                        logging.track_exception(*sys.exc_info(), properties={ 'ano': ano, 'mes' : mes, 'tienda': tienda })
                        logging.flush()
                        
                    
                        
        time.sleep(10)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())
    #crearXML("2022","06", "Tienda1002")