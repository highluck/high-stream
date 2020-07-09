package lee.high.stream.errors;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamDeserializationExceptionHandler implements DeserializationExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(StreamDeserializationExceptionHandler.class);

    @Override
    public DeserializationHandlerResponse handle(final ProcessorContext context,
                                                 final ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Exception caught during Deserialization, sending to the dead queue topic; " +
                "taskId: {}, topic: {}, partition: {}, offset: {}",
            context.taskId(), record.topic(), record.partition(), record.offset(),
            exception);

        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
