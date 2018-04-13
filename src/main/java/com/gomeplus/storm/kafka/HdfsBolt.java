package com.gomeplus.storm.kafka;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author tanyongkai
 * @purpose 替换原有的 org.apache.storm.hdfs.bolt.HdfsBolt
 *
 */
public class HdfsBolt extends AbstractHdfsBolt {
	private static final Logger LOG = LoggerFactory.getLogger(HdfsBolt.class);

	private transient FSDataOutputStream out;
	private RecordFormat format;
	
	//设置hdfs参数
	public HdfsBolt witHdfsConfigure(Map<String,String> confMap){
		this.confMap=confMap;
		return this;
	}
	
	//设置时间分割字段
	public HdfsBolt withSplitField(String field){
		this.split_field=field;
		return this;
	}
	//设置文件地址前缀
	public HdfsBolt withDirPrex(String dir_prex){
		this.dir_prex=dir_prex;
		return this;
	}

	public HdfsBolt withFsUrl(String fsUrl) {
		this.fsUrl = fsUrl;
		return this;
	}

	public HdfsBolt withConfigKey(String configKey) {
		this.configKey = configKey;
		return this;
	}

	public HdfsBolt withFileNameFormat(FileNameFormat fileNameFormat) {
		this.fileNameFormat = fileNameFormat;
		return this;
	}

	public HdfsBolt withRecordFormat(RecordFormat format) {
		this.format = format;
		return this;
	}

	public HdfsBolt withSyncPolicy(SyncPolicy syncPolicy) {
		this.syncPolicy = syncPolicy;
		return this;
	}

	public HdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
		this.rotationPolicy = rotationPolicy;
		return this;
	}

	public HdfsBolt addRotationAction(RotationAction action) {
		this.rotationActions.add(action);
		return this;
	}

	public HdfsBolt withTickTupleIntervalSeconds(int interval) {
		this.tickTupleInterval = interval;
		return this;
	}

	public HdfsBolt withRetryCount(int fileRetryCount) {
		this.fileRetryCount = fileRetryCount;
		return this;
	}

	@Override
	public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
		LOG.info("Preparing HDFS Bolt...");
		this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
	}

	@Override
	protected void syncTuples() throws IOException {
		LOG.debug("Attempting to sync all data to filesystem");
		if (this.out instanceof HdfsDataOutputStream) {
			((HdfsDataOutputStream) this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
		} else {
			this.out.hsync();
		}
	}

	@Override
	protected void writeTuple(Tuple tuple) throws IOException {
		
		byte[] bytes = this.format.format(tuple);
		out.write(bytes);
		this.offset += bytes.length;
		
	}
	/**
	 * 该方法已经被重写,每次关闭前先同步数据，再清空Batch
	 */
	@Override
	protected void closeOutputFile() throws IOException {
		//flush the batch before close
		syncTuples();
		clearBatch();
		this.out.close();
	}
	/**
	 * 该方法被改写，每次连接都必须在closeOutputFile之后
	 */
	@Override
	protected Path createOutputFile() throws IOException {
		Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, 0));

		//设置文件系统fs
		this.fs= FileSystem.get(this.hdfsConfig);
		
//		LOG.info("======="+this+" datedir:"+this.datedir + " tmpdatadir:"+this.tmpdatadir + "=======");
		//如果目录为空而且输出位置发生变化,则建立连接
		if (this.datedir == null || !this.tmpdatadir.equals(this.datedir)) {
			//check the dir is exist or not
			if (!fs.exists(path)) {
				if (out != null) {
					closeOutputFile();
				}
//				LOG.info("======="+this+" 1create out!"+"=======");
				//create output-dir
				this.out = fs.create(path);
				
			} else {
				if (out != null) {
					closeOutputFile();
				}
//				LOG.info("======="+this+" 2append out!"+"=======");
				//connect an exist dir
				this.out = fs.append(path);
				
			}
			this.datedir = this.tmpdatadir;//重置当前输出目录的标识位
		} 
//		else{
//			LOG.info("======= "+this.tmpdatadir+" = "+this.datedir+" =======");
//		}
		return path;
	}

}
