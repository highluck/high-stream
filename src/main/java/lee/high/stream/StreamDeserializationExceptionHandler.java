package lee.high.stream;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

public class StreamDeserializationExceptionHandler implements DeserializationExceptionHandler {

    @Override
    public DeserializationHandlerResponse handle(final ProcessorContext context,
                                                 final ConsumerRecord<byte[], byte[]> record, Exception exception) {
        System.out.println(String.format("Exception caught during Deserialization, sending to the dead queue topic; " +
                "taskId: %s, topic: %s, partition: %d, offset: %d",
            context.taskId(), record.topic(), record.partition(), record.offset(),
            exception));

        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
