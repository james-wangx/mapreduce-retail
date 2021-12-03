package com.pineapple.join;

import org.apache.hadoop.io.Writable;
import org.omg.CORBA_2_3.portable.OutputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class joinBean implements Writable {
    String transactionId;
    String storeId;
    String storeName;
    int reviewScore;
    int employeeNumber;

    public joinBean() {
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public int getReviewScore() {
        return reviewScore;
    }

    public void setReviewScore(int reviewScore) {
        this.reviewScore = reviewScore;
    }

    public int getEmployeeNumber() {
        return employeeNumber;
    }

    public void setEmployeeNumber(int employeeNumber) {
        this.employeeNumber = employeeNumber;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(transactionId);
        out.writeUTF(storeId);
        out.writeUTF(storeName);
        out.writeInt(reviewScore);
        out.writeInt(employeeNumber);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        transactionId = in.readUTF();
        storeId = in.readUTF();
        storeName = in.readUTF();
        reviewScore = in.readInt();
        employeeNumber = in.readInt();
    }

    @Override
    public String toString() {
        return storeId + "," + transactionId + "," + storeName + "," + reviewScore + "," + employeeNumber;
    }
}
