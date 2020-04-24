package com.scz.flk.model;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;

/**
 * Created by shen on 2019/12/22.
 */
public class OrderEvent implements Value {

    private long orderId;
    private String eventType = "";
    private String txId = "";
    private long eventTime;

    public OrderEvent() {};

    public OrderEvent(long orderId, String eventType, String txId, long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.eventTime = eventTime;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.orderId = in.readLong();
        this.eventType = in.readUTF();
        this.txId = in.readUTF();
        this.eventTime = in.readLong();
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeLong(this.orderId);
        out.writeUTF(this.eventType);
        out.writeUTF(this.txId);
        out.writeLong(this.eventTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OrderEvent order = (OrderEvent) o;

        if (orderId != order.orderId) return false;
        if (eventTime != order.eventTime) return false;
        if (!eventType.equals(order.eventType)) return false;
        return txId.equals(order.txId);
    }

    @Override
    public int hashCode() {
        int result = (int) (orderId ^ (orderId >>> 32));
        result = 31 * result + eventType.hashCode();
        result = 31 * result + txId.hashCode();
        result = 31 * result + (int) (eventTime ^ (eventTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", txId='" + txId + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
