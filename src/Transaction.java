public class Transaction {
    private String name;
    private boolean isRO;
    private Status tStatus;
    private int startTime;
    public Transaction(String name,boolean isRO,Status status,int startTime){
        this.name = name;
        this.isRO = isRO;
        this.tStatus = status;
        this.startTime = startTime;
    }
    
}
