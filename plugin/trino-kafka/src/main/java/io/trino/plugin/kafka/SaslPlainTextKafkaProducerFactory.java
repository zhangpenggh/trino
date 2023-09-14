/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka;

import com.google.inject.Inject;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class SaslPlainTextKafkaProducerFactory
        implements KafkaProducerFactory
{
    private final Set<HostAddress> nodes;
    private final SecurityProtocol securityProtocol;
    private final String mechanism;
    private final String saslJaasConfig;
    private final String acks;
    private final String compressionType;

    @Inject
    public SaslPlainTextKafkaProducerFactory(KafkaConfig kafkaConfig, KafkaSecurityConfig securityConfig)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        requireNonNull(securityConfig, "securityConfig is null");

        nodes = kafkaConfig.getNodes();
        securityProtocol = securityConfig.getSecurityProtocol().get();
        mechanism = securityConfig.getSaslMechanism();
        saslJaasConfig = securityConfig.getSaslJaasConfig();
        acks = kafkaConfig.getAcks();
        compressionType = kafkaConfig.getCompressionType();
    }

    @Override
    public Properties configure(ConnectorSession session)
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, nodes.stream()
                .map(HostAddress::toString)
                .collect(joining(",")));
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ACKS_CONFIG, acks);
        properties.setProperty(LINGER_MS_CONFIG, Long.toString(5));
        properties.setProperty(SECURITY_PROTOCOL_CONFIG, securityProtocol.name());
        properties.setProperty(SaslConfigs.SASL_MECHANISM, mechanism);
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        properties.setProperty(COMPRESSION_TYPE_CONFIG, compressionType);
        return properties;
    }
}
