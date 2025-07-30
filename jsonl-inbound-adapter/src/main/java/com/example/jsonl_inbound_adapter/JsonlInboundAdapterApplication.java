package com.example.jsonl_inbound_adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.json.JsonToObjectTransformer;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
public class JsonlInboundAdapterApplication {

    public static void main(String[] args) {
        SpringApplication.run(JsonlInboundAdapterApplication.class, args);
    }

}

@Configuration
class JsonIntegrationFlow {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Bean
    PropertiesPersistingMetadataStore metadataStore(@Value("file://${HOME}/Desktop/metadata") Resource baseDir)
            throws Exception {
        if (!baseDir.exists()) {
            baseDir.getFile().mkdirs();
        }
        var props = new PropertiesPersistingMetadataStore();
        props.setBaseDirectory(baseDir.getFile().getAbsolutePath());
        props.setFileName("metadata.properties");
        props.afterPropertiesSet();
        return props;
    }

//    @Bean
    IntegrationFlow tailIntegrationFlow(
            @Value("classpath:/seinfeld.jsonl") Resource jsonInput,
            @Value("file://${HOME}/Desktop/seinfeld.json") Resource output
    ) throws Exception {

        try (var fw = new FileWriter(output.getFile())) {
            var sampleJsonForDemo = jsonInput.getContentAsString(Charset.defaultCharset());
            FileCopyUtils.copy(sampleJsonForDemo, fw);
        }
        return IntegrationFlow.from(
                        Files.tailAdapter(output.getFile())
                                .autoStartup(true)
                                .end(false)
                )
                .transform(new JsonToObjectTransformer(JsonNode.class))
                .handle((payload, headers) -> {
                    log.debug("got a new line {} with type {}", payload, payload.getClass().getName());
                    return null;
                })
                .get();
    }


//    @Bean
    IntegrationFlow statefulJsonlIntegrationFlow(MetadataStore metadataStore, ObjectMapper objectMapper,
                                                 @Value("classpath:/seinfeld.jsonl") Resource jsonlFile) {
        Assert.state(jsonlFile.exists(), "the jsonl file must exist");
        var jsonl = new JsonInboundMessageProducer(objectMapper, metadataStore, jsonlFile);
        return IntegrationFlow
                .from(jsonl)
                .handle((GenericHandler<JsonNode>) (payload, _) -> {
                    log.info("got a new json node: {}", payload.get("name").asText());
                    return null;
                })
                .get();
    }

}

class JsonInboundMessageProducer extends AbstractMessageSource<JsonNode> {

    private final static String OFFSET_KEY = "offsetLine";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Resource jsonlFile;
    private final ObjectMapper objectMapper;
    private final MetadataStore metadataStore;
    private final Queue<JsonNode> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger offset = new AtomicInteger(0);

    JsonInboundMessageProducer(ObjectMapper objectMapper,
                               MetadataStore metadataStore,
                               Resource jsonlFile) {
        this.jsonlFile = jsonlFile;
        this.objectMapper = objectMapper == null ? new ObjectMapper() : objectMapper;
        this.metadataStore = metadataStore == null ? new SimpleMetadataStore() : metadataStore;
        Assert.notNull(jsonlFile, "jsonlFile must not be null");

    }

    private void start() throws Exception {
        var offsetString = this.metadataStore.get(OFFSET_KEY);
        var offsetLine = (offsetString == null ? 0 : Integer.parseInt(offsetString));
        this.offset.set(offsetLine);
        try (var randomAccessFile = new RandomAccessFile(this.jsonlFile.getFile(), "r");) {
            if (offsetLine > 0) {
                var currentLine = 0;
                do {
                    randomAccessFile.readLine();
                    currentLine += 1;
                } //
                while (currentLine < offsetLine);

                log.debug("starting at offset {}, {}, {}", offsetLine, randomAccessFile.getFilePointer(), randomAccessFile.length());
            }
            while (randomAccessFile.getFilePointer() < randomAccessFile.length()) {
                var next = randomAccessFile.readLine();
                if (next != null) {
                    var node = this.objectMapper.readTree(next);
                    this.queue.add(node);
                }
            }
        }
    }

    @Override
    protected void onInit() {
        try {
            this.started.set(true);
            this.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Object doReceive() {
        Assert.state(this.started.get(), "must be started");
        var node = this.queue.poll();
        if (node != null) {
            this.offset.incrementAndGet();
            this.metadataStore.put(OFFSET_KEY, String.valueOf(offset.get()));
        }
        return node;
    }

    @Override
    public String getComponentType() {
        return "jsonl:inbound-channel-adapter";
    }
}