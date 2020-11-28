import java.util.*;

public class Transaction {
    private final String name;
    private final boolean isRO;
    private Status tStatus;
    private final int startTime;
    private HashMap<String,String> operations;
    private final HashMap<String,String> locktable;

    // Used for ReadOnly Operations. Contains a snapshot of the database.
    // private HashMap<String,Integer> snapshot;
    // private int snapshotSite;

    public Transaction(String name,boolean isRO,Status status,int startTime){
        this.name = name;
        this.isRO = isRO;
        this.tStatus = status;
        this.startTime = startTime;
        locktable = new HashMap<>();
    }
    public void addOperation(String operation,String data){
        operations.put(operation,data);
    }

    // Returns whether a transaction is readonly
    public boolean isReadOnly(){
        return isRO;
    }

    public String getName(){
        return name;
    }

    public int getStartTime(){
        return startTime;
    }

    public HashMap<String,String> getLocktable() {
        return locktable;
    }

    public void addToLocktable(String variable, String type) {
        locktable.put(variable, type);
    }

    public void setStatus(Status status){
        tStatus = status;
    }

    public Status getStatus(){
        return tStatus;
    }
}
