import java.util.HashMap;
import java.util.Map;

public class Transaction {
    private String name;
    private boolean isRO;
    private Status tStatus;
    private int startTime;
    private HashMap<String,String> operations;
    private HashMap<String,String> locktable;

    // Used for ReadOnly Operations. Contains a snapshot of the database.
    private HashMap<String,Integer> snapshot;

    public Transaction(String name,boolean isRO,Status status,int startTime){
        this.name = name;
        this.isRO = isRO;
        this.tStatus = status;
        this.startTime = startTime;
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

    // Adds the data from the database to the snapshot
    public void addToSnapshot(Map<String,Integer> db){
        snapshot = new HashMap<String,Integer>(db);
    }

    // Gets the value for the variable for a read only transaction
    public int getVal(String variable){
        return snapshot.get(variable);
    }
}
