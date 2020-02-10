package com.wanghan;

public class Deal {
    private double price;
    private long amount;
    private long direction; //买卖方向
    private String bondCode;
    private String entityCode;
    private String counterEntityCode;
    private String srno; //成交编号
    private String quoteSrno; //对应的报价编号
    private long timestamp;

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public long getDirection() {
        return direction;
    }

    public void setDirection(long direction) {
        this.direction = direction;
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

    public String getCounterEntityCode() {
        return counterEntityCode;
    }

    public void setCounterEntityCode(String counterEntityCode) {
        this.counterEntityCode = counterEntityCode;
    }

    public String getSrno() {
        return srno;
    }

    public void setSrno(String srno) {
        this.srno = srno;
    }

    public String getQuoteSrno() {
        return quoteSrno;
    }

    public void setQuoteSrno(String quoteSrno) {
        this.quoteSrno = quoteSrno;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
