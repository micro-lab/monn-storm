package com.gomeplus.storm.bean;

import java.io.Serializable;

public class Login implements Serializable {
 
	private static final long serialVersionUID = 4141470371032906983L;
	private Long logTime;
    private Long userId;
    private Integer platformType;
    private String appId;

    private String logDay;

    public String getLogDay() {
        return logDay;
    }

    public void setLogDay(String logDay) {
        this.logDay = logDay;
    }

    public Long getLogTime() {
        return logTime;
    }

    public void setLogTime(Long logTime) {
        this.logTime = logTime;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Integer getPlatformType() {
        return platformType;
    }

    public void setPlatformType(Integer platformType) {
        this.platformType = platformType;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    @Override
    public String toString() {
        return userId+"|"+platformType+"|"+appId+"|"+logTime;
    }
}