import activemq
import util


settings = util.load_properties('secret-mq.properties')
username, password, broker, topic, subscription, client_id = settings['username'], settings['password'], settings['broker'], settings['topic'], settings['subscription'], settings['client_id']
durable = True


with activemq.Subscriber(username, password, broker, topic, subscription, client_id, durable) as client:
    for message in client:
        print(message)

