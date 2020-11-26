import java.util.HashMap;

public class Transaction {
    private String name;
    private boolean isRO;
    private Status tStatus;
    private int startTime;
    private HashMap<String,String> operations;
    public Transaction(String name,boolean isRO,Status status,int startTime){
        this.name = name;
        this.isRO = isRO;
        this.tStatus = status;
        this.startTime = startTime;
    }
    public void addOperation(String operation,String data){
        operations.put(operation,data);
    }
    
}
