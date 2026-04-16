package org.example;

public class PVMVLogType {
    public long eventTimeStamp;
    public String partitionLogChannel;
    public String partitionApp;
    public String sessionKey;
    public String requestKey;
    public Long sequenceKey;
    public String Log;
    public String logType;
    public PVMVFlowInfo pvmvFlowInfo;

    public static Long sequenceId = new Long(0);
    public PVMVLogType(){
        
    }

    public PVMVLogType(long eventTimeStamp) {
        this.eventTimeStamp = eventTimeStamp;
        long id = sequenceId % 1000;
        sequenceId += 1;
        this.sequenceKey = Thread.currentThread().getId() * 1000 + id;
        this.partitionLogChannel = "channel " + String.valueOf(id);
        this.partitionApp = "app " + String.valueOf(eventTimeStamp);
        this.requestKey = "request " + String.valueOf(id);
    }

    public PVMVLogType(long eventTimeStamp,long line){
        this.eventTimeStamp = eventTimeStamp/100;  //大概会有5000个相同的数
        long id = sequenceId % 10000;
        sequenceId += 1;
        this.sequenceKey = Thread.currentThread().getId() * 10000 + id;
        this.partitionLogChannel = "channel " + String.valueOf(line);
        this.partitionApp = "app " + String.valueOf(line);
        this.requestKey = "request " + String.valueOf(line);
        this.sessionKey = "session" + String.valueOf(line);
    }


    public PVMVFlowInfo getPvmvFlowInfo() {
        return pvmvFlowInfo;
    }

    public void setPvmvFlowInfo(PVMVFlowInfo pvmvFlowInfo) {
        this.pvmvFlowInfo = pvmvFlowInfo;
    }

    public long getEventTimeStamp() {
        return eventTimeStamp;
    }

    public void setEventTimeStamp(long eventTimeStamp) {
        this.eventTimeStamp = eventTimeStamp;
    }

    public String getPartitionLogChannel() {
        return partitionLogChannel;
    }

    public void setPartitionLogChannel(String partitionLogChannel) {
        this.partitionLogChannel = partitionLogChannel;
    }

    public String getPartitionApp() {
        return partitionApp;
    }

    public void setPartitionApp(String partitionApp) {
        this.partitionApp = partitionApp;
    }

    public String getSessionKey() {
        return sessionKey;
    }

    public void setSessionKey(String sessionKey) {
        this.sessionKey = sessionKey;
    }

    public String getRequestKey() {
        return requestKey;
    }

    public void setRequestKey(String requestKey) {
        this.requestKey = requestKey;
    }

    public Long getSequenceKey() {
        return sequenceKey;
    }

    public void setSequenceKey(Long sequenceKey) {
        this.sequenceKey = sequenceKey;
    }

    public String getLog() {
        return Log;
    }

    public void setLog(String log) {
        Log = log;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public static Long getSequenceId() {
        return sequenceId;
    }

    public static void setSequenceId(Long sequenceId) {
        PVMVLogType.sequenceId = sequenceId;
    }

    public String duplicateKey() {
        return this.getPartitionLogChannel() + ","
                + this.getPartitionApp() + ","
                + this.getSessionKey() + ","
                + this.getRequestKey() + ","
                + this.getSequenceKey();
    }

    public String joinKey() {
        return this.getPartitionLogChannel() + ","
                + this.getPartitionApp() + ","
                + this.getSessionKey() + ","
                + this.getRequestKey() + ",";
    }
}
