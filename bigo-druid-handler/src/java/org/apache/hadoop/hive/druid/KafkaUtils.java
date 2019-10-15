/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.druid;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * KafkaUtils
 */

public class KafkaUtils {

    private static KafkaProducer<String, String> createProducer(String brokerList) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerList);
        properties.put("retries", 3);
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>((properties));
    }

    public static void sendMessage(String brokerList, String topicName, String jsonMessage) {
        KafkaProducer<String, String> producer = createProducer(brokerList);
        producer.send(new ProducerRecord<String, String>(topicName, jsonMessage));
        producer.close();
    }

    public static void sendMessage(String brokerList, String topicName, String... jsonMessages) throws InterruptedException {
        KafkaProducer<String, String> producer = createProducer(brokerList);
        for (String jsonMessage : jsonMessages) {
            producer.send(new ProducerRecord<String, String>(topicName, jsonMessage));
        }
        producer.close();
    }

    public static void sendMessage(String brokerList, String topicName, List<String> jsonMessages) throws InterruptedException {
        KafkaProducer<String, String> producer = createProducer(brokerList);
        for (String jsonMessage : jsonMessages) {
            producer.send(new ProducerRecord<String, String>(topicName, jsonMessage));
        }
        producer.close();
    }

}
