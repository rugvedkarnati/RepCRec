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

    // Returns the name of the transaction.
    public String getName(){
        return name;
    }

    // Returns the start time of the transaction.
    public int getStartTime(){
        return startTime;
    }

    // Returns the locck table of the transaction.
    public HashMap<String,String> getLocktable() {
        return locktable;
    }

    // Adds new lock to the lock table.
    public void addToLocktable(String variable, String type) {
        locktable.put(variable, type);
    }

    // Changes the status of the transaction.
    public void setStatus(Status status){
        tStatus = status;
    }

    // Returns the status of the transaction.
    public Status getStatus(){
        return tStatus;
    }
}
