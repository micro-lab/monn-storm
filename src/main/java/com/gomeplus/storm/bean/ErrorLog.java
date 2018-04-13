package com.gomeplus.storm.bean;

import java.io.Serializable;

public class ErrorLog implements Serializable{
	private static final long serialVersionUID = -1641962467893738790L;
	private long logTime;  		//13位长整形时间戳
	private int serverType;		//服务类型
	private String ip;			//ip
	private int port;			//端口
	private String level;		//错误级别
	private String msg;			//日志信息
	private String errorStackTrace;	//堆栈信息
	private String logDate;		//字符串时间
	
	
	public long getLogTime() {
		return logTime;
	}
	public void setLogTime(long logTime) {
		this.logTime = logTime;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getLevel() {
		return level;
	}
	public void setLevel(String level) {
		this.level = level;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public String getErrorStackTrace() {
		return errorStackTrace;
	}
	public void setErrorStackTrace(String errorStackTrace) {
		this.errorStackTrace = errorStackTrace;
	}
	public int getServerType() {
		return serverType;
	}
	public void setServerType(int serverType) {
		this.serverType = serverType;
	}
	public String getLogDate() {
		return logDate;
	}
	public void setLogDate(String logDate) {
		this.logDate = logDate;
	}
	
	
	
}
