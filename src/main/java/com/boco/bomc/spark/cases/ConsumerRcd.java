package com.boco.bomc.spark.cases;

import java.io.Serializable;

public class ConsumerRcd implements Serializable {
    /**
     *          交易单号 交易日期      产品种类    产品价格 用户ID
     * 数据格式：1       2000-04-12   P001       ￥123   123
     */
    private String did;
    private String dealDate;
    private String prodId;
    private String prodPrice;
    private String uid;

    public ConsumerRcd(String did, String dealDate, String prodId, String prodPrice, String uid) {
        this.did = did;
        this.dealDate = dealDate;
        this.prodId = prodId;
        this.prodPrice = prodPrice;
        this.uid = uid;
    }

    public String getDid() {
        return did;
    }

    public void setDid(String did) {
        this.did = did;
    }

    public String getDealDate() {
        return dealDate;
    }

    public void setDealDate(String dealDate) {
        this.dealDate = dealDate;
    }

    public String getProdId() {
        return prodId;
    }

    public void setProdId(String prodId) {
        this.prodId = prodId;
    }

    public String getProdPrice() {
        return prodPrice;
    }

    public void setProdPrice(String prodPrice) {
        this.prodPrice = prodPrice;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }
}
