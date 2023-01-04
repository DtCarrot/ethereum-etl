import collections
import json
import logging
import os

from kafka import KafkaProducer

from blockchainetl.jobs.exporters.converters.composite_item_converter import (
    CompositeItemConverter,
)


class KafkaItemExporter:
    def __init__(self, output, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        # Break query string if there is any
        self.connection_url = self.get_connection_url(output)
        named_args = {}
        protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
        if protocol == "SASL_PLAINTEXT":
            username = os.getenv("KAFKA_USERNAME")
            password = os.getenv("KAFKA_PASSWORD")
            named_args = {
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                # SASL username
                "sasl_plain_username": username,
                # SASL password
                "sasl_plain_password": password,
            }

        self.producer = KafkaProducer(
            bootstrap_servers=self.connection_url, **named_args
        )

    def get_connection_url(self, output):
        try:
            return output.split("/")[1]
        except KeyError:
            raise Exception(
                'Invalid kafka output param, It should be in format of "kafka/127.0.0.1:9092"'
            )

    def open(self):
        pass

    def export_items(self, items):
        for item in items:
            self.export_item(item)

    def export_item(self, item):
        item_type = item.get("type")
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            data = json.dumps(item).encode("utf-8")
            logging.debug(data)
            return self.producer.send(
                self.item_type_to_topic_mapping[item_type], value=data
            )
        else:
            logging.warning(
                'Topic for item type "{}" is not configured.'.format(item_type)
            )

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        pass


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get("type")].append(item)

    return result
