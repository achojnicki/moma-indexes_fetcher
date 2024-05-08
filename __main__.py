from adistools.adisconfig import adisconfig
from adistools.log import Log

from pika import BlockingConnection, PlainCredentials, ConnectionParameters
from websockets.sync.client import connect as websocket_connect
from functools import partial
from json import dumps



class fetcher:
    project_name = "moma-indexes_fetcher"

    def __init__(self):
        self.active=True
        self.websocket_conn=None



        self.config = adisconfig('/opt/adistools/configs/moma-indexes_fetcher.yaml')
        self.log = Log(
            parent=self,
            rabbitmq_host=self.config.rabbitmq.host,
            rabbitmq_port=self.config.rabbitmq.port,
            rabbitmq_user=self.config.rabbitmq.user,
            rabbitmq_passwd=self.config.rabbitmq.password,
            debug=self.config.log.debug,
        )

        self.rabbitmq_conn = BlockingConnection(
            ConnectionParameters(
                host=self.config.rabbitmq.host,
                port=self.config.rabbitmq.port,
                credentials=PlainCredentials(
                    self.config.rabbitmq.user,
                    self.config.rabbitmq.password
                )
            )
        )

        self.rabbitmq_channel = self.rabbitmq_conn.channel()

        self.websocket_connect()



    def websocket_connect(self):
        msg='{"type": "subscribe","subscriptions":[{"name":"candles_1m","symbols":["BTCUSD"]}]}'
        self.websocket_conn=websocket_connect(
            uri="wss://api.gemini.com/v2/marketdata"
            )

        self.websocket_conn.send(msg)


    def loop(self):
        while self.active:
            result=self.websocket_conn.recv()

            self.rabbitmq_channel.basic_publish(
                exchange="",
                routing_key="moma-indexes_fetched",
                body=result
            )


    def start(self):
        self.loop()

if __name__ == "__main__":
    worker = fetcher()
    worker.start()
