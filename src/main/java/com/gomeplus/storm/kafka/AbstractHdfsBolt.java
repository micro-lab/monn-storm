package com.gomeplus.storm.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author tanyongkai
 * @purpose 用于替换原有的@AbstractHdfsBolt
 *
 */
public abstract class AbstractHdfsBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHdfsBolt.class);
    private static final Integer DEFAULT_RETRY_COUNT = 3;
    /**
     * Half of the default Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS
     */
    private static final int DEFAULT_TICK_TUPLE_INTERVAL_SECS = 15;

    protected ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();
    private Path currentFile;
    protected OutputCollector collector;
    protected transient FileSystem fs;
    protected SyncPolicy syncPolicy;
    protected FileRotationPolicy rotationPolicy;
    protected FileNameFormat fileNameFormat;
    protected int rotation = 0;
    protected String fsUrl; 
    protected String configKey;
    protected transient Object writeLock;
    protected transient Timer rotationTimer; // only used for TimedRotationPolicy
    private List<Tuple> tupleBatch = new LinkedList<Tuple>();
    protected long offset = 0;
    protected Integer fileRetryCount = DEFAULT_RETRY_COUNT;
    protected Integer tickTupleInterval = DEFAULT_TICK_TUPLE_INTERVAL_SECS;
    
    //判断位,判断输出位置是否发生变化
    protected String datedir;
    protected String tmpdatadir;
//    protected boolean outflag=false;
    
    //location output-dir
    protected String dir_prex; //输出路径前缀
    protected String split_field; //获取判定时间的字段
    protected transient Configuration hdfsConfig;
    protected Map<String,String> confMap;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); 

    protected void rotateOutputFile() throws IOException {
        LOG.info("Rotating output file...");
        long start = System.currentTimeMillis();
        synchronized (this.writeLock) {
        	//don't close!
//            closeOutputFile();
//            this.rotation++;
        	
            Path newFile = createOutputFile();
            LOG.info("Performing {} file rotation actions.", this.rotationActions.size());
            for (RotationAction action : this.rotationActions) {
                action.execute(this.fs, this.currentFile);
            }
            this.currentFile = newFile;
        }
        long time = System.currentTimeMillis() - start;
        LOG.info("File rotation took {} ms.", time);
    }

    /**
     * Marked as final to prevent override. Subclasses should implement the doPrepare() method.
     * @param conf
     * @param topologyContext
     * @param collector
     */
    public final void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector){
        this.writeLock = new Object();
        if (this.syncPolicy == null) throw new IllegalStateException("SyncPolicy must be specified.");
        if (this.rotationPolicy == null) throw new IllegalStateException("RotationPolicy must be specified.");
        if (this.fsUrl == null) {
            throw new IllegalStateException("File system URL must be specified.");
        }

        this.collector = collector;
        this.fileNameFormat.prepare(conf, topologyContext);
        this.hdfsConfig = new Configuration();
        
        for(String key:confMap.keySet()){
			hdfsConfig.set(key, confMap.get(key));
		}
      
        //hdfsConfig不再通过默认配置获取，直接在hdfsbolt初始化时直接设置
//        Map<String, Object> map = (Map<String, Object>)conf.get(this.configKey);
//        if(map != null){
//            for(String key : map.keySet()){
//                this.hdfsConfig.set(key, String.valueOf(map.get(key)));
//            }
//        }
        

        
        try{
            HdfsSecurityUtil.login(conf, hdfsConfig);
            doPrepare(conf, topologyContext, collector);
            this.currentFile = createOutputFile();

        } catch (Exception e){
            throw new RuntimeException("Error preparing HdfsBolt: " + e.getMessage(), e);
        }

        if(this.rotationPolicy instanceof TimedRotationPolicy){
            long interval = ((TimedRotationPolicy)this.rotationPolicy).getInterval();
            this.rotationTimer = new Timer(true);
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    try {
                        rotateOutputFile();
                    } catch(IOException e){
                        LOG.warn("IOException during scheduled file rotation.", e);
                    }
                }
            };
            this.rotationTimer.scheduleAtFixedRate(task, interval, interval);
        }
    }
    
    //clear beach and reset the syncPolicySize
    protected void clearBatch(){
    	for (Tuple t : tupleBatch) {
            this.collector.ack(t);
        }
    	tupleBatch.clear();
    	 syncPolicy.reset();
    }
    
    public final void execute(Tuple tuple) {


    	
    	if(!tuple.getSourceStreamId().equals("__tick")){
        	//获取元组中的时间
        	Date splitDate = new Date(Long.valueOf(tuple.getStringByField(this.split_field)));
    //	
        	String dateStr = sdf.format(splitDate);
//        	//拼装输出路径
        	String outDir = this.dir_prex + dateStr +"/";
        	//hdfsBolt设置输出路径
        	((DefaultFileNameFormat)fileNameFormat).withPath(outDir);
//        	((DefaultFileNameFormat)(fileNameFormat)).withPath(this.dir_prex + sdf.format(new Date(Long.valueOf(tuple.getStringByField(this.split_field))))+"/");
        	
        	//判断日期是否发生变化，是否需要更改输出路径
        	this.tmpdatadir = dateStr;
//    		this.tmpdatadir=sdf.format(new Date(Long.valueOf(tuple.getStringByField(this.split_field))));
    	}
    	
        synchronized (this.writeLock) {
            boolean forceSync = false;
            if (TupleUtils.isTick(tuple)) {
                LOG.debug("TICK! forcing a file system flush");
                this.collector.ack(tuple);
                forceSync = true;
            } else {
                try {
                	try {
        				rotateOutputFile();
        			} catch (IOException e) {
        				e.printStackTrace();
        			}
                
                    writeTuple(tuple);
                    tupleBatch.add(tuple);
                } catch (IOException e) {
                    //If the write failed, try to sync anything already written
                    LOG.info("Tuple failed to write, forcing a flush of existing data.");
                    this.collector.reportError(e);
                    forceSync = true;
                    this.collector.fail(tuple);
                }
            }

            if (this.syncPolicy.mark(tuple, this.offset) || (forceSync && tupleBatch.size() > 0)) {
                int attempts = 0;
                boolean success = false;
                IOException lastException = null;
                // Make every attempt to sync the data we have.  If it can't be done then kill the bolt with
                // a runtime exception.  The filesystem is presumably in a very bad state.
                while (success == false && attempts < fileRetryCount) {
                    attempts += 1;
                    try {
                    	
                        syncTuples();
                        LOG.debug("Data synced to filesystem. Ack'ing [{}] tuples", tupleBatch.size());
                        clearBatch();
                        
                        success = true;
                    } catch (IOException e) {
                        LOG.warn("Data could not be synced to filesystem on attempt [{}]", attempts);
                        this.collector.reportError(e);
                        lastException = e;
                    }
                }

                // If unsuccesful fail the pending tuples
                if (success == false) {
                    LOG.warn("Data could not be synced to filesystem, failing this batch of tuples");
                    for (Tuple t : tupleBatch) {
                        this.collector.fail(t);
                    }
                    tupleBatch.clear();

                    throw new RuntimeException("Sync failed [" + attempts + "] times.", lastException);
                }
            }
            
//            if(this.offset >= this.syncPolicy.mark(tuple, offset))
            
            if(this.rotationPolicy.mark(tuple, this.offset)) {
                try {
                	
                	this.rotation++;
                    rotateOutputFile();
                    this.rotationPolicy.reset();
                    this.offset = 0;
                } catch (IOException e) {
                    this.collector.reportError(e);
                    LOG.warn("File could not be rotated");
                    //At this point there is nothing to do.  In all likelihood any filesystem operations will fail.
                    //The next tuple will almost certainly fail to write and/or sync, which force a rotation.  That
                    //will give rotateAndReset() a chance to work which includes creating a fresh file handle.
                }
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), tickTupleInterval);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    /**
     * writes a tuple to the underlying filesystem but makes no guarantees about syncing data.
     *
     * this.offset is also updated to reflect additional data written
     *
     * @param tuple
     * @throws IOException
     */
    abstract protected void writeTuple(Tuple tuple) throws IOException;

    /**
     * Make the best effort to sync written data to the underlying file system.  Concrete classes should very clearly
     * state the file state that sync guarantees.  For example, HdfsBolt can make a much stronger guarantee than
     * SequenceFileBolt.
     *
     * @throws IOException
     */
    abstract protected void syncTuples() throws IOException;

    abstract protected void closeOutputFile() throws IOException;

    abstract protected Path createOutputFile() throws IOException;

    abstract protected void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException;

}
