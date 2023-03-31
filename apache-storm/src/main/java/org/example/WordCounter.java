package org.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Fields;


public class WordCounter extends BaseBasicBolt {
    private Map<String, Integer> counts = new HashMap<>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // Recupera a palavra da tupla
        String word = input.getString(0);

        // Verifica se a palavra é válida (não é vazia)
        if (StringUtils.isNotBlank(word)) {
            // Incrementa o contador para essa palavra
            int count = counts.getOrDefault(word, 0);
            counts.put(word, count + 1);

            // Emite a palavra e sua contagem
            collector.emit(new Values(word, count + 1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Declara a saída como dois campos: "word" e "count"
        declarer.declare(new Fields("word", "count"));
    }
}

