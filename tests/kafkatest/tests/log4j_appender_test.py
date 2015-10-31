# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from ducktape.utils.util import wait_until
from ducktape.tests.test import Test
from ducktape.mark import matrix

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka_log4j_appender import KafkaLog4jAppender
from kafkatest.utils.security_config import SecurityConfig

TOPIC = "topic-log4j-appender"
MAX_MESSAGES = 100

class Log4jAppenderTest(Test):
    """
    Tests KafkaLog4jAppender using VerifiableKafkaLog4jAppender that appends increasing ints to a Kafka topic
    """
    def __init__(self, test_context):
        super(Log4jAppenderTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.topics = {
            TOPIC: {'partitions': 1, 'replication-factor': 1}
        }

        self.zk = ZookeeperService(test_context, self.num_zk)

    def setUp(self):
        self.zk.start()

    def start_kafka(self, security_protocol, interbroker_security_protocol):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            self.zk, security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol, topics=self.topics)
        self.kafka.start()

    def start_appender(self, security_protocol):
        self.appender = KafkaLog4jAppender(self.test_context, self.num_brokers, self.kafka, TOPIC, MAX_MESSAGES,
                                           security_protocol=security_protocol)
        self.appender.start()

    def start_consumer(self, security_protocol):
        enable_new_consumer = security_protocol == SecurityConfig.SSL
        self.consumer = ConsoleConsumer(self.test_context, num_nodes=self.num_brokers, kafka=self.kafka, topic=TOPIC,
                                        consumer_timeout_ms=1000, new_consumer=enable_new_consumer, security_protocol=security_protocol)
        self.consumer.start()

    @matrix(security_protocol=['PLAINTEXT', 'SSL'])
    def test_log4j_appender(self, security_protocol='PLAINTEXT'):
        """
        Tests if KafkaLog4jAppender is producing to Kafka topic
        :return: None
        """
        self.start_kafka(security_protocol, security_protocol)
        self.start_appender(security_protocol)
        self.appender.wait()

        self.start_consumer(security_protocol)
        node = self.consumer.nodes[0]

        wait_until(lambda: self.consumer.alive(node),
            timeout_sec=10, backoff_sec=.2, err_msg="Consumer was too slow to start")

        # Verify consumed messages count
        expected_lines_count = MAX_MESSAGES * 2  # two times to account for new lines introduced by log4j
        wait_until(lambda: len(self.consumer.messages_consumed[1]) == expected_lines_count, timeout_sec=10,
                   err_msg="Timed out waiting to consume expected number of messages.")

        self.consumer.stop()