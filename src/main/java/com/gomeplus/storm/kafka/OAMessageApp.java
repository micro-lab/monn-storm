package com.gomeplus.storm.kafka;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author tanyongkai
 * @purpose 企办消息日志数据清洗程序入口
 */
public class OAMessageApp {
	
	public static void main(String[] args) throws Exception {
		//配置spout的kafka信息
        String zks = "bj02-im-hdp01.pro.gomeplus.com:2181,bj02-im-hdp02.pro.gomeplus.com:2181,bj02-im-hdp03.pro.gomeplus.com:2181,bj02-im-hdp04.pro.gomeplus.com:2181,bj02-im-hdp05.pro.gomeplus.com:2181";
//		String zks = "bj02-im-hadoop01:2181,bj02-im-hadoop02:2181,bj02-im-hadoop03:2181,bj02-im-hadoop04:2181,bj02-im-hadoop05:2181";
//		String zks = "bj01-im-data01:2181,bj01-im-data02:2181,bj01-im-data03:2181";
        String topic = "oa_chaos_log";
        
        //配置用于记录storm消费位移的zk目录
        String zkRoot = "/kafkaspout_offest";
        String id = "oa_message_log";
        
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        
        //配置用于记录storm消费位移的zk,可以和kafka的zk分离
//      spoutConf.zkServers = Arrays.asList("bj02-im-hadoop01,bj02-im-hadoop02,bj02-im-hadoop03,bj02-im-hadoop04,bj02-im-hadoop05".split(","));//bj02-im-hadoop01,bj02-im-hadoop02,bj02-im-hadoop03,bj02-im-hadoop04,bj02-im-hadoop05
        spoutConf.zkServers = Arrays.asList("bj02-im-hdp01.pro.gomeplus.com,bj02-im-hdp02.pro.gomeplus.com,bj02-im-hdp03.pro.gomeplus.com,bj02-im-hdp04.pro.gomeplus.com,bj02-im-hdp05.pro.gomeplus.com".split(","));
		spoutConf.zkPort = 2181;
      
       	//配置spout的输出类型
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        //第一次运行程序不需要这两行代码,第二次运行时使用，使storm的消费偏移量强制写入zk,以后无需使用
//      spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
//      spoutConf.ignoreZkOffsets =true;

		
        
        //配置hdfsbolt
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);//每1000条记录同步一次
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\t").withRecordDelimiter("\n");//hdfs文件的格式，以\t 分割，以\n换行
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/");//文件名策略,初始化流的输出地址
//		FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.DAYS); // TimeRotationPolicy
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1,Units.GB);	//文件大小达到1G时，强制换文件
		
			//配置hdfs信息,使用以下配置,确保ha模式下，hdfs能够以append模式写入 
		   Map<String,String> hdfsConfig = new HashMap<String,String>();
			hdfsConfig.put("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
			hdfsConfig.put("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
			hdfsConfig.put("dfs.support.append", "true");
	        hdfsConfig.put("fs.defaultFS", "hdfs://namenodeHacluster");
			hdfsConfig.put("dfs.nameservices", "namenodeHacluster");
			hdfsConfig.put("dfs.ha.namenodes.namenodeHacluster", "nn1,nn2");
			hdfsConfig.put("dfs.namenode.rpc-address.namenodeHacluster.nn1", "bj02-im-hdp01.pro.gomeplus.com:9000");//pre:bj02-im-hadoop01  //pro:bj02-im-hdp01.pro.gomeplus.com   //dev:bj01-im-data01
			hdfsConfig.put("dfs.namenode.rpc-address.namenodeHacluster.nn2", "bj02-im-hdp02.pro.gomeplus.com:9000");
			hdfsConfig.put("dfs.client.failover.proxy.provider.namenodeHacluster",  
					"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		
		//定义message 日志的输出信息
		HdfsBolt messageBolt = new HdfsBolt()
				.withFsUrl("hdfs://namenodeHacluster") //hdfs地址
				.withFileNameFormat(fileNameFormat)	 //文件名格式,/user/hive/xx.txt,初始化输出流的地址，最终被重定向
				.withRecordFormat(format)				 //文件格式
                .withRotationPolicy(rotationPolicy)	 //文件切分策略
                .withSyncPolicy(syncPolicy)			 //同步策略
				.withDirPrex("/user/hive1.2.1/warehouse/oa.db/message_log/logday=") //输出文件的地址
				.withSplitField("messageTime") //messageTime 
				.witHdfsConfigure(hdfsConfig);
		
		//定义login 日志的输出信息  (目前生产尚未更新login日志,使用老版本)
		HdfsBolt loginBolt = new HdfsBolt()
                .withFsUrl("hdfs://namenodeHacluster") 
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .withDirPrex("/user/hive1.2.1/warehouse/oa.db/login_log/logday=")
                .withSplitField("logtime")//loginTime  //logtime old
                .witHdfsConfigure(hdfsConfig);
		
		//定义group 日志的输出信息 
		HdfsBolt groupBolt = new HdfsBolt()
		        .withFsUrl("hdfs://namenodeHacluster") 
		        .withFileNameFormat(fileNameFormat)
		        .withRecordFormat(format)
		        .withRotationPolicy(rotationPolicy)
		        .withSyncPolicy(syncPolicy)
		        .withDirPrex("/user/hive1.2.1/warehouse/oa.db/group_log/logday=")
		        .withSplitField("operationTime")
		        .witHdfsConfigure(hdfsConfig);
		
		//定义group 日志的输出信息 
		HdfsBolt fileBolt = new HdfsBolt()
		        .withFsUrl("hdfs://namenodeHacluster") 
		        .withFileNameFormat(fileNameFormat)
		        .withRecordFormat(format)
		        .withRotationPolicy(rotationPolicy)
		        .withSyncPolicy(syncPolicy)
		        .withDirPrex("/user/hive1.2.1/warehouse/oa.db/file_log/logday=")
		        .withSplitField("operationTime")
		        .witHdfsConfigure(hdfsConfig);
		
		
		//配置kafkaBolt信息
		Properties props = new Properties();
		//bj01-im-data01:9092,bj01-im-data02:9092,bj01-im-data03:9092
		//bj02-im-hadoop01:9092,bj02-im-hadoop02:9092,bj02-im-hadoop03:9092,bj02-im-hadoop04:9092,bj02-im-hadoop05:9092
		//bj02-im-hdp01.pro.gomeplus.com:9092,bj02-im-hdp02.pro.gomeplus.com:9092,bj02-im-hdp03.pro.gomeplus.com:9092,bj02-im-hdp04.pro.gomeplus.com:9092,bj02-im-hdp05.pro.gomeplus.com:9092
		props.put("bootstrap.servers", "bj02-im-hdp01.pro.gomeplus.com:9092,bj02-im-hdp02.pro.gomeplus.com:9092,bj02-im-hdp03.pro.gomeplus.com:9092,bj02-im-hdp04.pro.gomeplus.com:9092,bj02-im-hdp05.pro.gomeplus.com:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("topic", "log");
		KafkaBolt kkBolt = new KafkaBolt().withProducerProperties(props);

		
		
		//创建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(spoutConf),4);
        builder.setBolt("vBolt",new MessageBolt(),4).shuffleGrouping("kafka-spout");
        builder.setBolt("message-bolt", messageBolt,8).shuffleGrouping("vBolt","message");
        builder.setBolt("login-bolt", loginBolt,4).shuffleGrouping("vBolt","login");
        builder.setBolt("group-bolt", groupBolt,4).shuffleGrouping("vBolt","group");
        builder.setBolt("file-bolt", fileBolt,4).shuffleGrouping("vBolt","file");
        builder.setBolt("log-kafka-bolt", kkBolt,4).shuffleGrouping("vBolt","log");
        
        Config config = new Config();
        //设置并行度
        config.setNumWorkers(16);
       
        //设置kafkaBolt的输出topic
        config.put("topic", "log");
        
        //设置拓扑名
        String name = "storm_to_hdfs";
        
        //提交任务
//      new LocalCluster().submitTopology(name, config, builder.createTopology());
        StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology()); 	

            
//      Thread.sleep(60000);
//      cluster.shutdown();
        
   }
		

	
}
