package org.example;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordReader extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<String> queue;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };
    private int index = 0;


    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.queue = new LinkedBlockingQueue<String>();
        for (String sentence : sentences) {
            queue.offer(sentence);
        }
    }

    @Override
    public void nextTuple() {
        if (index < sentences.length) {
            String sentence = sentences[index];
            collector.emit(new Values(sentence));
            index++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}

