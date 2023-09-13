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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;

public class KafkaSecurityConfig
{
    private SecurityProtocol securityProtocol = PLAINTEXT;
    private String saslMechanism;
    private String saslJaasConfig;

    public Optional<SecurityProtocol> getSecurityProtocol()
    {
        return Optional.ofNullable(securityProtocol);
    }

    @Config("kafka.security-protocol")
    @ConfigDescription("Kafka communication security protocol")
    public KafkaSecurityConfig setSecurityProtocol(SecurityProtocol securityProtocol)
    {
        this.securityProtocol = securityProtocol;
        return this;
    }

    @Config("kafka.sasl-mechanism")
    @ConfigDescription("Kafka communication sasl mechanism")
    public KafkaSecurityConfig setSaslMechanism(String saslMechanism)
    {
        this.saslMechanism = saslMechanism;
        return this;
    }

    @Config("kafka.sasl-jaas-config")
    @ConfigDescription("Kafka communication sasl jaas config")
    public KafkaSecurityConfig setSaslJaasConfig(String saslJaasConfig)
    {
        this.saslJaasConfig = saslJaasConfig;
        return this;
    }

    @PostConstruct
    public void validate()
    {
        checkState(
                securityProtocol.equals(PLAINTEXT) || securityProtocol.equals(SSL) || securityProtocol.equals(SASL_PLAINTEXT),
                format("Only %s and %s and %s security protocols are supported", PLAINTEXT, SSL, SASL_PLAINTEXT));
    }

    public String getSaslMechanism()
    {
        return saslMechanism;
    }

    public String getSaslJaasConfig()
    {
        return saslJaasConfig;
    }
}
