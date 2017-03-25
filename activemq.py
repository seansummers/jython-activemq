import json

from javax.jms.Session import (
    SESSION_TRANSACTED,
    AUTO_ACKNOWLEDGE,
    CLIENT_ACKNOWLEDGE,
    DUPS_OK_ACKNOWLEDGE,
)
from org.apache.activemq import ActiveMQConnectionFactory


def _factory(
        brokerURL=None,
        userName=None,
        password=None,
        clientID=None,
        useCompression=True,
        alwaysSyncSend=True
):
    connection_factory = ActiveMQConnectionFactory
    if not (userName and password):
        factory = connection_factory(brokerURL) if brokerURL else connection_factory()
    else:
        factory = connection_factory(userName, password, brokerURL)
    if clientID:
        factory.clientID = clientID
    factory.useCompression = useCompression
    factory.alwaysSyncSend = alwaysSyncSend
    # factory.useAsyncSend = True
    return factory


def _connection(factory, clientID=None, userName=None, password=None):
    # http://activemq.apache.org/maven/apidocs/org/apache/activemq/ActiveMQConnectionFactory.html#setClientID-java.lang.String-
    # says to set clientID per connection
    # http://docs.oracle.com/javaee/6/api/javax/jms/Connection.html#setClientID(java.lang.String)
    # says the preferred way to assign a clientID is to set it in the ConnectionFactory
    create_connection = factory.createConnection
    connection = create_connection(userName, password
                             ) if userName and password else create_connection()
    if clientID:
        connection.clientID = clientID
    return connection


def _session(connection, acknowledgeMode=AUTO_ACKNOWLEDGE, transacted=False):
    # http://docs.oracle.com/javaee/6/api/javax/jms/TopicSession.html
    #  'In general, use the Session object, and use TopicSession only to support existing code.'
    transacted = transacted or acknowledgeMode == SESSION_TRANSACTED
    args = (transacted, SESSION_TRANSACTED if transacted else awknowledgeMode)
    session = connection.createSession(*args)
    return session


def _publisher(session, topic):
    destination = session.createTopic(topic)
    publisher = session.createProducer(destination)
    return publisher


def _subscriber(
        session, topic, subscriber_name=None, noLocal=True, durable=False
):
    destination = session.createTopic(topic)
    if subscriber_name and durable:
        # TopicSubscriber is a subclass of MessageConsumer
        subscription = session.createDurableSubscriber(
            destination, subscriber_name, None, noLocal
        )
    elif not durable:
        # MessageConsumer
        subscription = session.createConsumer(destination, None, noLocal)
    else:
        err = "Invalid Subscription: client={}, topic={}, subscriber_name={}, durable_subscriber={}"
        raise ValueError(
            err.format(
                session.connection.clientID, topic, subscriber_name, durable
            )
        )
    return subscription


class ActiveMQ(object):

    def __init__(self, user_name, password, broker_url, client_id=None):
        factory = _factory(broker_url, user_name, password, client_id)
        connection = _connection(factory)
        self._session = _session(connection)
        self._connection = connection

    def close(self):
        if hasattr(self, '_obj'):
            self._obj.close()
        self._session.close()
        self._connection.stop()
        self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class Publisher(ActiveMQ):

    def __init__(self, user_name, password, broker_url, topic):
        super(self.__class__, self).__init__(user_name, password, broker_url)
        self._obj = _publisher(self._session, topic)

    def send(self, message):
        if not isinstance(message, basestring):
            json.dumps(message, separators=(',', ':'))
        msg = self._session.createTextMessage(message)
        self._obj.send(msg)


class Subscriber(ActiveMQ):

    def __init__(
            self,
            user_name,
            password,
            broker_url,
            topic,
            subscription=None,
            client_id=None,
            durable=False,
            timeout=0
    ):
        super(self.__class__, self).__init__(
            user_name, password, broker_url, client_id
        )
        self.durable = durable
        self.subscription = subscription
        self.timeout = timeout
        self._obj = _subscriber(
            self._session, topic, subscription, durable_subscriber=durable
        )

    def unsubscribe(self):
        self._obj.close()
        self._session.unsubscribe(self.subscription)

    def receive(self, timeout=0):
        timeout = timeout or self.timeout
        self._connection.start()
        message = self._obj.receive(timeout)
        self._connection.stop()
        if not message:
            raise StopIteration
        return message

    def __iter__(self):
        return self

    def next(self):
        return self.receive()

    def send(self, timeout=0):
        self.timeout = timeout
        self.next()
