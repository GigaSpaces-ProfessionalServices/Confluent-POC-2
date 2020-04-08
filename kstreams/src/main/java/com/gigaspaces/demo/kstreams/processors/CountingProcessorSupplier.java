package com.gigaspaces.demo.kstreams.processors;

import com.gigaspaces.demo.kstreams.gks.GigaStateStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;

public class CountingProcessorSupplier implements ProcessorSupplier<String, String> {

    @Override
    public Processor<String, String> get() {
        return new CountingProcessor();
    }


    private static final class CountingProcessor implements Processor<String, String> {

        private final Logger logger = LoggerFactory.getLogger(CountingProcessor.class);


        private ProcessorContext context;
        private GigaStateStore<String,Long> store;

        public CountingProcessor() {

        }

        @Override
        public void init(ProcessorContext processorContext) {
            this.context = processorContext;
            store = (GigaStateStore) context.getStateStore(GigaStateStore.STORE_NAME);
        }

        @Override
        public void process(final String key, String words) {
            HashMap<String, Long> wordCount = new HashMap<>();

            String[] values = words.split("\\s+");
            for(int i=0; i < values.length; i++) {
                String word = values[i];
                Long count = store.read(word);
                if (count == null) {
                    store.write(word,1L);
                }else {
                    store.write(word, count + 1);
                }
            };
        }

        @Override
        public void close() {

        }
    }
}