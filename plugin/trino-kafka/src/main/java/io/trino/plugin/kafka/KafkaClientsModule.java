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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.kafka.security.ForKafkaSsl;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class KafkaClientsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(KafkaSecurityConfig.class);
        installClientModule(SecurityProtocol.PLAINTEXT, KafkaClientsModule::configurePlainText);
        installClientModule(SecurityProtocol.SSL, KafkaClientsModule::configureSsl);
        installClientModule(SecurityProtocol.SASL_PLAINTEXT, KafkaClientsModule::configureSaslPlainText);
    }

    private void installClientModule(SecurityProtocol securityProtocol, Module module)
    {
        install(conditionalModule(
                KafkaSecurityConfig.class,
                config -> config.getSecurityProtocol().equals(securityProtocol),
                module));
    }

    private static void configurePlainText(Binder binder)
    {
        binder.bind(KafkaConsumerFactory.class).to(PlainTextKafkaConsumerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaProducerFactory.class).to(PlainTextKafkaProducerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaAdminFactory.class).to(PlainTextKafkaAdminFactory.class).in(Scopes.SINGLETON);
    }

    private static void configureSsl(Binder binder)
    {
        binder.bind(KafkaConsumerFactory.class).annotatedWith(ForKafkaSsl.class).to(PlainTextKafkaConsumerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaProducerFactory.class).annotatedWith(ForKafkaSsl.class).to(PlainTextKafkaProducerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaAdminFactory.class).annotatedWith(ForKafkaSsl.class).to(PlainTextKafkaAdminFactory.class).in(Scopes.SINGLETON);

        binder.bind(KafkaConsumerFactory.class).to(SslKafkaConsumerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaProducerFactory.class).to(SslKafkaProducerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaAdminFactory.class).to(SslKafkaAdminFactory.class).in(Scopes.SINGLETON);
    }

    private static void configureSaslPlainText(Binder binder)
    {
        binder.bind(KafkaConsumerFactory.class).to(SaslPlainTextKafkaConsumerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaProducerFactory.class).to(SaslPlainTextKafkaProducerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaAdminFactory.class).to(SaslPlainTextKafkaAdminFactory.class).in(Scopes.SINGLETON);
    }
}
