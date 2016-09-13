package io.confluent.examples.streams.pt;

import io.confluent.examples.streams.noc.TrafficMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Created by lmagdic on 13/09/16.
 */
public class PtDruidAggProcessor implements Processor<String,TrafficMessage> {

    private ProcessorContext context;
    private KeyValueStore<String,PtDruidAgg> druidStore;

    @Override
    public void init(ProcessorContext processorContext) {
        context = processorContext;

        //context.schedule(1000); // is it each 1000 messages ?

        druidStore = (KeyValueStore) context.getStateStore("druid-store");
    }

    @Override
    public void process(String key, TrafficMessage ptMessage){

        PtDruidAgg old = druidStore.get(key);
        if (old != null) {
            old.setCount(-old.getCount());  // inverse count
            //System.out.println("old="+old);
            context.forward(key, old);
        }

        PtDruidAgg druidAgg = new PtDruidAgg(ptMessage.getStatusId(),1);
        druidStore.put(key,druidAgg);
        context.forward(key, druidAgg);
    }

    @Override
    public void punctuate(long timestamp) {}

    @Override
    public void close(){
        druidStore.close();
    }
}
