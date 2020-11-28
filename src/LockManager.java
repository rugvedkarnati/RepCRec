import java.util.*;

public class LockManager {

    // LockTuple class is used to store the type of lock and the transactions which hold the lock
    private class LockTuple{
        ArrayList<String> transactionList;
        String lockType;
        LockTuple(String transaction,String lockType){
            transactionList = new ArrayList<>();
            transactionList.add(transaction);
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
    public Result getReadLock(String transaction, String variable){
        Result result = new Result(false,-1,"");
        LockTuple locktuple = lockTable.get(variable);
        if(locktuple == null){
            LockTuple newlocktuple = new LockTuple(transaction,"R");
            lockTable.put(variable, newlocktuple);
            result.status = true;
        }
        else if(locktuple.lockType.compareTo("R") == 0 || (locktuple.lockType.compareTo("W") == 0 && locktuple.transactionList.contains(transaction))){
            lockTable.get(variable).transactionList.add(transaction);
            result.status = true;
        }
        else{
            result.transaction = locktuple.transactionList.get(0);
            result.status = false;
        }
        return result;
    }

    // getWriteLock returns true if the transactions can get the lock
    // for the given variable or else it returns false.
    // If the variable is not locked transaction is added to the locktable.
    public Result getWriteLock(String transaction, String variable){
        Result result = new Result(false,-1,"");
        LockTuple locktuple = lockTable.get(variable);
        if(locktuple == null){
            LockTuple newlocktuple = new LockTuple(transaction,"W");
            lockTable.put(variable, newlocktuple);
            result.status = true;
        }
        else if(locktuple.transactionList.contains(transaction) && locktuple.transactionList.size() == 1){
            locktuple.lockType = "W";
            result.status = true;
        }
        else{
            result.transaction = locktuple.transactionList.get(0);
            result.status = false;
        }
        return result;
    }

    public void removeLock(String variable,String t){
        if(!lockTable.containsKey(variable)) return;
        lockTable.get(variable).transactionList.remove(t);
        if(lockTable.get(variable).transactionList.isEmpty()){
            lockTable.remove(variable);
        }
    }

    public void abortornot(Map<String,Transaction> transactions){
        System.out.println("hello");
        System.out.println(lockTable);
        lockTable.forEach((K,V) -> {
            System.out.println(K+V);
            V.transactionList.forEach((T) ->{
                System.out.println(transactions.get(T));
                transactions.get(T).setStatus(Status.TO_BE_ABORTED);
            });
        });
    }

}
