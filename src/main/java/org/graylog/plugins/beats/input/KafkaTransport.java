package org.graylog.plugins.beats.input;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.transports.ThrottleableTransport;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.journal.RawMessage;
import org.graylog2.plugin.lifecycles.Lifecycle;
import org.graylog2.plugin.system.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.codahale.metrics.MetricRegistry.name;

public class KafkaTransport extends ThrottleableTransport {

    public static final String GROUP_ID = "graylog2";
    public static final String CK_FETCH_MIN_BYTES = "fetch_min_bytes";
    public static final String CK_FETCH_WAIT_MAX = "fetch_wait_max";
    public static final String CK_BOOTSTRAP_SERVER = "bootstrap_servers";
    public static final String CK_POOL_TIMEOUT = "pool_timeout";
    public static final String CK_TOPIC_FILTER = "topic_filter";
    public static final String CK_THREADS = "threads";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTransport.class);

    private final Configuration configuration;
    private final MetricRegistry localRegistry;
    private final NodeId nodeId;
    private final EventBus serverEventBus;
    private final ServerStatus serverStatus;
    private final ScheduledExecutorService scheduler;
    private final MetricRegistry metricRegistry;
    private final AtomicLong totalBytesRead = new AtomicLong(0);
    private final AtomicLong lastSecBytesRead = new AtomicLong(0);
    private final AtomicLong lastSecBytesReadTmp = new AtomicLong(0);
    private final Random randomGenerator = new Random();

    private volatile boolean stopped = false;
    private ExecutorService executor;
    private List<KafkaConsumerRunner> runnerList = Lists.newArrayList();

    @AssistedInject
    public KafkaTransport(@Assisted Configuration configuration,
                          LocalMetricRegistry localRegistry,
                          NodeId nodeId,
                          EventBus serverEventBus,
                          ServerStatus serverStatus,
                          @Named("daemonScheduler") ScheduledExecutorService scheduler) {
        super(serverEventBus, configuration);
        this.configuration = configuration;
        this.localRegistry = localRegistry;
        this.nodeId = nodeId;
        this.serverEventBus = serverEventBus;
        this.serverStatus = serverStatus;
        this.scheduler = scheduler;
        this.metricRegistry = localRegistry;

        localRegistry.register("read_bytes_1sec", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return lastSecBytesRead.get();
            }
        });
        localRegistry.register("written_bytes_1sec", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return 0L;
            }
        });
        localRegistry.register("read_bytes_total", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return totalBytesRead.get();
            }
        });
        localRegistry.register("written_bytes_total", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return 0L;
            }
        });
    }

    @Subscribe
    public void lifecycleStateChange(Lifecycle lifecycle) {
        LOG.debug("Lifecycle changed to {}", lifecycle);
        switch (lifecycle) {
            case PAUSED:
            case FAILED:
            case HALTING:
                if (!executor.isShutdown() || !executor.isTerminated()) {
                    doStop();
                }
                break;
            default:
                //nothing todo ??
                break;
        }
    }

    @Override
    public void setMessageAggregator(CodecAggregator ignored) {
    }

    @Override
    public void doLaunch(final MessageInput input) throws MisfireException {
        serverStatus.awaitRunning(() -> {
            lifecycleStateChange(Lifecycle.RUNNING);
        });

        // listen for lifecycle changes
        serverEventBus.register(this);

        final Properties props = new Properties();
        //new prop
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getString(CK_BOOTSTRAP_SERVER));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(configuration.getInt(CK_FETCH_MIN_BYTES)));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(configuration.getInt(CK_FETCH_WAIT_MAX)));
        // Default auto commit interval in 0.9 is 5 seconds.  Reduce to 1 second to minimize message duplication
        // if something breaks as previous version
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");


        final int numThreads = configuration.getInt(CK_THREADS);
        executor = executorService(numThreads);

        for (int i = 0; i < numThreads; i++) {
            KafkaConsumerRunner runner = new KafkaConsumerRunner(props, input, configuration.getString(CK_TOPIC_FILTER), configuration.getInt(CK_POOL_TIMEOUT));
            runnerList.add(runner);
            executor.submit(runner);
        }

        scheduler.scheduleAtFixedRate(() -> {
            lastSecBytesRead.set(lastSecBytesReadTmp.getAndSet(0));
        }, 1, 1, TimeUnit.SECONDS);

    }

    private ExecutorService executorService(int numThreads) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("kafka-transport-%d").build();
        return new InstrumentedExecutorService(
                Executors.newFixedThreadPool(numThreads, threadFactory),
                metricRegistry,
                name(this.getClass(), "executor-service"));
    }

    @Override
    public void doStop() {
        stopped = true;
        serverEventBus.unregister(this);
        runnerList.forEach(runner -> {
            try {
                runner.shutdown();
            } catch (InterruptedException e) {
                LOG.error("", e);
            }
        });
        executor.shutdown();
    }

    @Override
    public MetricSet getMetricSet() {
        return localRegistry;
    }

    @FactoryClass
    public interface Factory extends Transport.Factory<KafkaTransport> {
        @Override
        KafkaTransport create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config extends ThrottleableTransport.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest cr = super.getRequestedConfiguration();

            cr.addField(new TextField(
                    CK_BOOTSTRAP_SERVER,
                    "Broker server",
                    "localhost:9092",
                    "A list host/port pair used to establishing the initial connection to the Kafka cluster",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new NumberField(
                    CK_POOL_TIMEOUT,
                    "Consumer time out",
                    100,
                    "The time in millis spent waiting in poll if data is not available",
                    ConfigurationField.Optional.OPTIONAL));

            cr.addField(new TextField(
                    CK_TOPIC_FILTER,
                    "Topic Name",
                    "your-topic",
                    "List of Topic name will be consumed. char separator need to be \';\'",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new NumberField(
                    CK_FETCH_MIN_BYTES,
                    "Fetch minimum bytes",
                    5,
                    "Wait for a message batch to reach at least this size or the configured maximum wait time before fetching.",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new NumberField(
                    CK_FETCH_WAIT_MAX,
                    "Fetch maximum wait time (ms)",
                    100,
                    "Wait for this time or the configured minimum size of a message batch before fetching.",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new NumberField(
                    CK_THREADS,
                    "Processor threads",
                    2,
                    "Number of processor threads to spawn. Use one thread per Kafka topic partition. if has more node use number_of_thread = number_topic / number_of_node",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            return cr;
        }
    }

    class KafkaConsumerRunner implements Runnable {
        private final CountDownLatch shutdownLatch;
        private final KafkaConsumer<byte[], byte[]> consumer;
        private final Properties props;
        private final MessageInput input;
        private final String topic;
        private final int poolTimeOut;

        public KafkaConsumerRunner(Properties props, MessageInput input, String topic, int poolTimeOut) {
            this.props = props;
            this.props.put("client.id", "gl2-" + nodeId + "-" + input.getId() + "-" + randomGenerator.nextInt(100));
            this.input = input;
            this.topic = topic;
            this.poolTimeOut = poolTimeOut;
            consumer = new KafkaConsumer<>(this.props);
            shutdownLatch = new CountDownLatch(1);
        }


        @Override
        public void run() {
            try {
                consumer.subscribe(Arrays.asList(topic));
                while (true) {

                    if (isThrottled()) {
                        blockUntilUnthrottled();
                    }
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(poolTimeOut);
                    records.forEach(record -> doProcessRecord(record));
                }
            } catch (WakeupException e) {

            } finally {
                consumer.close();
                shutdownLatch.countDown();
            }
        }


        protected void doProcessRecord(ConsumerRecord<byte[], byte[]> record) {

            if (record.value() != null) {
                LOG.trace("Receive message: {}, Partition: {}, Offset: {}, by ThreadID: {}", deserialize(record.value()), record.partition(), record.offset(), Thread.currentThread().getId());
                final RawMessage rawMessage = new RawMessage(record.value());
                // TODO implement throttling
                input.processRawMessage(rawMessage);
            }

        }


        private String deserialize(byte[] objectData) {
            try {
                return objectData == null ? StringUtils.EMPTY : new String(objectData, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return StringUtils.EMPTY;
        }


        public void shutdown() throws InterruptedException {
            consumer.wakeup();
            shutdownLatch.await();
        }

    }

}
