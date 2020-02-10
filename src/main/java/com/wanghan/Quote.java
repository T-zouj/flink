package com.wanghan;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

public class Quote {

    private double askPrice;
    private double bidPrice;
    private long bidAmount;
    private long askAmount;
    private String bondCode;
    private String entityCode;
    private String srno;
    private long timestamp;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public String getSrno() {
        return srno;
    }

    public void setSrno(String srno) {
        this.srno = srno;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getAskPrice() {
        return askPrice;
    }

    public void setAskPrice(double askPrice) {
        this.askPrice = askPrice;
    }

    public double getBidPrice() {
        return bidPrice;
    }

    public void setBidPrice(double bidPrice) {
        this.bidPrice = bidPrice;
    }

    public String getBondCode() {
        return bondCode;
    }

    public void setBondCode(String bondCode) {
        this.bondCode = bondCode;
    }

    public String getEntityCode() {
        return entityCode;
    }

    public void setEntityCode(String entityCode) {
        this.entityCode = entityCode;
    }

    public long getBidAmount() {
        return bidAmount;
    }

    public void setBidAmount(long bidAmount) {
        this.bidAmount = bidAmount;
    }

    public long getAskAmount() {
        return askAmount;
    }

    public void setAskAmount(long askAmount) {
        this.askAmount = askAmount;
    }
}
