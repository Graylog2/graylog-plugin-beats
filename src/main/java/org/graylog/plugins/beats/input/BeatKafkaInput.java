package org.graylog.plugins.beats.input;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog.plugins.beats.BeatsCodec;
import org.graylog2.inputs.codecs.GelfCodec;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;

import javax.inject.Inject;

/**
 * Created by fbalicchia on 10/06/17.
 */
public class BeatKafkaInput extends MessageInput {
    private static final String NAME = "Beat Kafka plugin";

    @AssistedInject
    public BeatKafkaInput(@Assisted Configuration configuration,
                          MetricRegistry metricRegistry,
                          KafkaTransport.Factory transport,
                          BeatsCodec.Factory codec,
                          LocalMetricRegistry localRegistry,
                          BeatKafkaInput.Config config,
                          BeatKafkaInput.Descriptor descriptor, ServerStatus serverStatus) {
        this(metricRegistry,
                configuration,
                transport.create(configuration),
                codec.create(configuration),
                localRegistry,
                config,
                descriptor, serverStatus);
    }

    protected BeatKafkaInput(MetricRegistry metricRegistry,
                             Configuration configuration,
                             KafkaTransport kafkaTransport,
                             BeatsCodec codec,
                             LocalMetricRegistry localRegistry,
                             MessageInput.Config config,
                             MessageInput.Descriptor descriptor, ServerStatus serverStatus) {
        super(metricRegistry, configuration, kafkaTransport, localRegistry, codec, config, descriptor, serverStatus);
    }


    @Override
    public Boolean isGlobal() {
        return super.isGlobal();
    }

    @FactoryClass
    public interface Factory extends MessageInput.Factory<BeatKafkaInput> {
        @Override
        BeatKafkaInput create(Configuration configuration);

        @Override
        BeatKafkaInput.Config getConfig();

        @Override
        BeatKafkaInput.Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageInput.Descriptor {
        @Inject
        public Descriptor() {
            super(NAME, false, "");
        }
    }

    @ConfigClass
    public static class Config extends MessageInput.Config {
        @Inject
        public Config(KafkaTransport.Factory transport, GelfCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }
}
