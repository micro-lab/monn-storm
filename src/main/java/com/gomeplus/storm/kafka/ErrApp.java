package com.gomeplus.storm.kafka;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
//import org.elasticsearch.storm.EsBolt;

/**
 * @author tanyongkai
 * @purpose 应用日志数据清洗程序入口
 */
public class ErrApp {

	public static void main(String[] args) throws Exception {
		
		 //配置spout的kafka信息
//		String zks = "bj02-im-hadoop01:2181,bj02-im-hadoop02:2181,bj02-im-hadoop03:2181,bj02-im-hadoop04:2181,bj02-im-hadoop05:2181";
		String zks = "bj02-im-hdp01.pro.gomeplus.com:2181,bj02-im-hdp02.pro.gomeplus.com:2181,bj02-im-hdp03.pro.gomeplus.com:2181,bj02-im-hdp04.pro.gomeplus.com:2181,bj02-im-hdp05.pro.gomeplus.com:2181";
		String topic = "chaos_error_log";
		
		//配置用于记录storm消费位移的zk目录
        String zkRoot = "/kafkaspout_offest";
        String id = "error_log"; 
        
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        //配置spout的输出类型
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        //配置用于记录storm消费位移的zk,可以和kafka的zk分离
//        spoutConf.zkServers = Arrays.asList("bj02-im-hadoop01,bj02-im-hadoop02,bj02-im-hadoop03,bj02-im-hadoop04,bj02-im-hadoop05".split(","));
        spoutConf.zkServers = Arrays.asList("bj02-im-hdp01.pro.gomeplus.com,bj02-im-hdp02.pro.gomeplus.com,bj02-im-hdp03.pro.gomeplus.com,bj02-im-hdp04.pro.gomeplus.com,bj02-im-hdp05.pro.gomeplus.com".split(","));
        spoutConf.zkPort=2181;
          
        //第一次运行程序不需要这两行代码,第二次运行时使用，使storm的消费偏移量强制写入zk,以后无需使用
//        spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
//        spoutConf.ignoreZkOffsets =true;
        KafkaSpout kafkaSpout =  new KafkaSpout(spoutConf);
        

       
        //配置esBolt
        Config boltConf = new Config();
        boltConf.put("es.cluster.name", "im_es");
        boltConf.put("es.nodes", "10.125.149.52:9200,10.125.149.53:9200,10.125.149.54:9200");//pre:10.125.196.51  //pro:10.125.149.52:9200,10.125.149.53:9200,10.125.149.54:9200
        boltConf.put("es.index.auto.create", "true");
        boltConf.put("es.ser.writer.bytes.class", "org.elasticsearch.storm.serialization.StormTupleBytesConverter");//es序列化类型  
        boltConf.put("es.input.json", "true"); //es输入数据类型
        
        //是否在程序中开启手动提交
//        boltConf.put("es.storm.bolt.write.ack", "true");
        //每次手动提交的数据量
//        boltConf.put("es.storm.bolt.flush.entries.size", 1000);
        
        
        //是否使用内部提交
//        boltConf.put("es.storm.bolt.tick.tuple.flush",false);
        //EsBolt自动提交的时间间隔
//        boltConf.put("topology.tick.tuple.freq.secs", 10);
        
        //强制刷新的时间间隔
        boltConf.put("es.force.flush.Millis", 60000L);
        
        
        EsBolt esBolt = new EsBolt("imlog/errorlog",boltConf); //申明 index/type

        
        //构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout,4);
        builder.setBolt("error-bolt", new ErrorBolt(),4).shuffleGrouping("kafka-spout");
        builder.setBolt("es-bolt", esBolt,4).shuffleGrouping("error-bolt");
        
        Config config = new Config();
        //设置并行度
        config.setNumWorkers(4);

        //提交任务
//        new LocalCluster().submitTopology("storm_to_es", config, builder.createTopology());//local
        StormSubmitter.submitTopologyWithProgressBar("storm_to_es", config, builder.createTopology());//cluster 

	}

}
