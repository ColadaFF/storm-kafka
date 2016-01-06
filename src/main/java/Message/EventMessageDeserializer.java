package Message;

import backtype.storm.tuple.Values;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;

public class EventMessageDeserializer extends BaseFunction {
    public EventMessageDeserializer() {

    }

    public EventMessage DeserializeEvent(byte[] event) throws IOException {
        SpecificDatumReader<EventMessage> reader = new SpecificDatumReader<EventMessage>(EventMessage.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(event, null);
        return reader.read(null, decoder);
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        byte[] logLine = tridentTuple.getBinary(0);
        try {
            EventMessage eventMessage = DeserializeEvent(logLine);
            tridentCollector.emit(new Values(eventMessage));
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
