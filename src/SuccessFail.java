public class SuccessFail {
    public boolean status;

    // This variable will be used when returning from a read operation.
    // It contains the data requested if status is success.
    public int value;

    // This variable will be used when the operation to the site is not successful beacuse of locks.
    // It stores the transaction holding the locks.
    public String transaction;
    
    public SuccessFail(boolean status,int value,String transaction){
        this.status = status;
        this.value = value;
        this.transaction = transaction;
    }
}
