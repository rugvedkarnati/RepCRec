import java.util.ArrayList;
import java.util.HashMap;

public class LockManager {
    private class LockTuple{
        ArrayList<Transaction> transactionList;
        String lockType;
        LockTuple(Transaction t,String lockType){
            transactionList = new ArrayList<>();
            transactionList.add(t);
            this.lockType = lockType;
        }
        // public void add(Transaction t){
        //     transactionList.add(t);
        // }
    }
    public HashMap<String,LockTuple> lockTable;
    public LockManager(){
        lockTable = new HashMap<>();
    }
    public SuccessFail getReadLock(Transaction t, String variable){
        SuccessFail s = new SuccessFail(false,-1,t.getName());
        LockTuple t1 = lockTable.get(variable);
        if(t1 == null){
            LockTuple l = new LockTuple(t,"R");
            lockTable.put(variable, l);
            s.status = true;
        }
        else if(t1.lockType.compareTo("R") == 0){
            lockTable.get(variable).transactionList.add(t);
            s.status = true;
        }
        else{
            s.transaction = t1.transactionList.get(0).getName();
            s.status = false;
        }
        return s;
    }
    public SuccessFail getWriteLock(Transaction t, String variable){
        SuccessFail s = new SuccessFail(false,-1,t.getName());
        LockTuple t1 = lockTable.get(variable);
        if(t1 == null){
            LockTuple l = new LockTuple(t,"W");
            lockTable.put(variable, l);
            s.status = true;
        }
        else{
            s.transaction = t1.transactionList.get(0).getName();
            s.status = false;
        }
        return s;
    }
}
