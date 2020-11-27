import java.util.ArrayList;
import java.util.Map;
import java.util.List;
public class Site {
    public enum SiteStatus{ACTIVE,FAIL,RECOVER};
    DataManager dm;
    LockManager lm;
    public SiteStatus status;
    public Site(){
        dm = new DataManager();
        lm = new LockManager();
    }

    // Checks whether the transaction can get a lock for the given variable.
    // It returns the data if the transaction can get a lock or else it returns false.
    // Two cases where it cannot get a lock- 
    // Either the site is down or else some other transaction holds the lock.
    public SuccessFail readdata(String t,String variable){
        SuccessFail s = new SuccessFail(false,-1,t);
        if(status == SiteStatus.ACTIVE){
            s = lm.getReadLock(t,variable);
            if(s.status){
                s.value = dm.readCommitData(variable);
            }
        }
        else{
            s.status = false;
        }
        return s;
    }

    // Checks whether the transaction can get a lock for the given variable.
    public SuccessFail canGetWriteLock(String t,String variable){
        return lm.getWriteLock(t,variable);
    }

    // Writes data to the database for this site using the datamanager.
    // Returns False if the site is down.
    public SuccessFail writedata(String t,String variable,int value){
        SuccessFail s = new SuccessFail(false,-1,t.getName());
        if(status == SiteStatus.ACTIVE){
            s = lm.getWriteLock(t,variable);
            if(s.status){
                s.status = dm.writeData(variable,value);
            }
        }
        else{
            s.status = false;
        }
        return s;
    }

    public void changeStatus(SiteStatus status){
        this.status = status;
    }
    public SiteStatus getStatus(){
        return status;
    }

    // Returns the copy of the database
    public Map<String,Integer> getDB(){
        return dm.getDB();
    }

    // Removes locks for the given variable held by the given transaction
    public void removeLock(String variable, String t,boolean commit,int time){
        if(commit){
            dm.commit(variable, time);
        }
        lm.removeLock(variable, t);
    }

    public List<Integer> finddata(String variable,int time){
        return dm.findData(variable, time);
    }

    public void abortornot(Map<String,Transaction> transactions){
        lm.abortornot(transactions);
    }
    // public ArrayList<Integer> getCommitTime(String variable){
    //     return dm.getcommitTime(variable);
    // }

    public void commit(String transaction){
        lm.getVar(transaction).foreach();
    }
}
