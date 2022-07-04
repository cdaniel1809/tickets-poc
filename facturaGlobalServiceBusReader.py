import json
import asyncio
from connectionController import *
from facturaGlobalXMLCreator import *
from azure.servicebus import ServiceBusClient
import logging

from opencensus.trace import config_integration
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.tracer import Tracer


from opencensus.ext.azure.log_exporter import AzureLogHandler


config_integration.trace_integrations(['logging'])
logging.basicConfig(format='%(asctime)s traceId=%(traceId)s spanId=%(spanId)s %(message)s')
tracer = Tracer(sampler=AlwaysOnSampler())

logger = logging.getLogger(__name__)
logger.addHandler(AzureLogHandler(connection_string='InstrumentationKey=d85f6718-2588-4168-b8d9-0d9f1ac6b9d8'))
logger.warning('Iniciando el proceso de generación de XML Global!')

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
                        
                        logger.warning("Received: " + str(msg))
                        receiver.complete_message(msg)
                        logger.warning(f"Inicia la generacion de XML : ano : {ano}, mes : {mes}, tienda : {tienda} ")

                        crearXML(ano,mes,tienda )
                        
                        logger.warning(f"Se ha generado el XML : ano : {ano}, mes : {mes}, tienda : {tienda} ")
                        
                        
                    except Exception as e:
                        properties={ 'ano': ano, 'mes' : mes, 'tienda': tienda }
                        logger.exception('Captured an exception : ' , extra=properties)
                                  
        time.sleep(10)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())
    #crearXML("2022","06", "Tienda1002")