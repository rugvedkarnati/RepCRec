import java.util.HashMap;
import java.util.Map;
public class Transaction {
    private String name;
    private boolean isRO;
    private Status tStatus;
    private int startTime;
    private HashMap<String,String> operations;
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
    public boolean isReadOnly(){
        return isRO;
    }
    public String getName(){
        return name;
    }
    public void addToSnapshot(Map<String,Integer> db){
        snapshot = new HashMap<String,Integer>(db);
    }
    public int getVal(String variable){
        return snapshot.get(variable);
    }
}
