
package com.gomeplus.storm.kafka;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.PartitionWriter;
import org.elasticsearch.storm.TupleUtils;
import org.elasticsearch.storm.cfg.StormSettings;
import org.elasticsearch.storm.serialization.StormTupleBytesConverter;
import org.elasticsearch.storm.serialization.StormTupleFieldExtractor;
import org.elasticsearch.storm.serialization.StormValueWriter;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.*;
import static org.elasticsearch.storm.cfg.StormConfigurationOptions.ES_STORM_BOLT_ACK;


/**
 * 
 * @author tanyongkai
 * @purpose 替换Esbolt,添加一个自动刷新线程
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class EsBolt extends Thread implements IRichBolt {

	private transient static Log log = LogFactory.getLog(EsBolt.class);

	private Map boltConfig = new LinkedHashMap();

	private transient PartitionWriter writer;
	private transient boolean flushOnTickTuple = true;
	private transient boolean ackWrites = false;

	private transient List<Tuple> inflightTuples = null;
	private transient int numberOfEntries = 0;
	private transient OutputCollector collector;

	private transient long flushMillis = 0;

	public EsBolt(String target) {
		boltConfig.put(ES_RESOURCE_WRITE, target);
	}

	public EsBolt(String target, boolean writeAck) {
		boltConfig.put(ES_RESOURCE_WRITE, target);
		boltConfig.put(ES_STORM_BOLT_ACK, Boolean.toString(writeAck));
	}

	public EsBolt(String target, Map configuration) {
		boltConfig.putAll(configuration);
		boltConfig.put(ES_RESOURCE_WRITE, target);
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		LinkedHashMap copy = new LinkedHashMap(conf);
		copy.putAll(boltConfig);

		StormSettings settings = new StormSettings(copy);
		flushOnTickTuple = settings.getStormTickTupleFlush();
		ackWrites = settings.getStormBoltAck();

		// trigger manual flush
		if (ackWrites) {
			settings.setProperty(ES_BATCH_FLUSH_MANUAL, Boolean.TRUE.toString());

			// align Bolt / es-hadoop batch settings
			numberOfEntries = settings.getStormBulkSize();
			settings.setProperty(ES_BATCH_SIZE_ENTRIES, String.valueOf(numberOfEntries));

			inflightTuples = new ArrayList<Tuple>(numberOfEntries + 1);
		}

		int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();

		InitializationUtils.setValueWriterIfNotSet(settings, StormValueWriter.class, log);
		InitializationUtils.setBytesConverterIfNeeded(settings, StormTupleBytesConverter.class, log);
		InitializationUtils.setFieldExtractorIfNotSet(settings, StormTupleFieldExtractor.class, log);

		writer = RestService.createWriter(settings, context.getThisTaskIndex(), totalTasks, log);

		// 获取自动刷新的时间
		flushMillis = (long) copy.get("es.force.flush.Millis");
		// 开启自动刷新进程
		start();
	}

	@Override
	public void execute(Tuple input) {
		if (flushOnTickTuple && TupleUtils.isTickTuple(input)) {
			flush();
			return;
		}
		if (ackWrites) {
			inflightTuples.add(input);
		}
		try {
			writer.repository.writeToIndex(input);

			// manual flush in case of ack writes - handle it here.
			if (numberOfEntries > 0 && inflightTuples.size() >= numberOfEntries) {
				flush();
			}

			if (!ackWrites) {
				collector.ack(input);
			}
		} catch (RuntimeException ex) {
			if (!ackWrites) {
				collector.fail(input);
			}
			throw ex;
		}
	}

	private void flush() {
		if (ackWrites) {
			flushWithAck();
		} else {
			flushNoAck();
		}
	}

	private void flushWithAck() {
		BitSet flush = null;

		try {
			flush = writer.repository.tryFlush().getLeftovers();
		} catch (EsHadoopException ex) {
			// fail all recorded tuples
			for (Tuple input : inflightTuples) {
				collector.fail(input);
			}
			inflightTuples.clear();
			throw ex;
		}

		for (int index = 0; index < inflightTuples.size(); index++) {
			Tuple tuple = inflightTuples.get(index);
			// bit set means the entry hasn't been removed and thus wasn't
			// written to ES
			if (flush.get(index)) {
				collector.fail(tuple);
			} else {
				collector.ack(tuple);
			}
		}

		// clear everything in bulk to prevent 'noisy' remove()
		inflightTuples.clear();
	}

	private void flushNoAck() {
		writer.repository.flush();
	}

	@Override
	public void cleanup() {
		if (writer != null) {
			try {
				flush();
			} finally {
				writer.close();
				writer = null;
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	
	/**
	 * 定时刷新线程
	 */
	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(flushMillis);
			} catch (InterruptedException e) {
				log.info(e.getMessage());
			}
			flush();
		}
	}

}
