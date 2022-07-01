from azure.servicebus import ServiceBusClient, ServiceBusMessage
from connectionController import *
import sys
import json

def send_batch_message(sender):
    i = int(sys.argv[1])
    max = int(sys.argv[2])
    batch_message = sender.create_message_batch()
    while i <= max:
        try:
            JSON = { "ano" : "2022", "mes":"06", "tienda": f"{i}" }
            json_object = json.dumps(JSON)
            message = ServiceBusMessage(json_object) 
            sender.send_messages(message)
        except ValueError:
            # ServiceBusMessageBatch object reaches max_size.
            # New ServiceBusMessageBatch object can be created here to send more data.
            break
        i += 1    
    
    print(f"Sent a batch of {max} messages")


servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)
with servicebus_client:
    sender = servicebus_client.get_topic_sender(topic_name=TOPIC_NAME)
    with sender:
        send_batch_message(sender)

print("Done sending messages")
print("-----------------------")