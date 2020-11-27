import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LockManager {

    // LockTuple class is used to store the type of lock and the transactions which hold the lock
    private class LockTuple{
        ArrayList<Transaction> transactionList;
        String lockType;
        LockTuple(Transaction t,String lockType){
            transactionList = new ArrayList<>();
            transactionList.add(t);
            this.lockType = lockType;
        }
    }

    // lockTable contains the locks held by different transactions for different variables
    public Map<String,LockTuple> lockTable;

    public LockManager(){
        lockTable = new HashMap<>();
    }

    // getReadLock returns true if the transactions can get the lock
    // for the given variable or else it returns false. 
    // If the variable is not locked transaction is added to the locktable.
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

    // getWriteLock returns true if the transactions can get the lock
    // for the given variable or else it returns false.
    // If the variable is not locked transaction is added to the locktable.
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
