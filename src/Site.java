import java.util.Map;
public class Site {
    public enum SiteStatus{ACTIVE,FAIL,RECOVER};
    DataManager dm;
    LockManager lm;
    public SiteStatus status;
    public Site(){
        dm = new DataManager();
        lm = new LockManager();
    }
    public SuccessFail readdata(Transaction t,String variable){
        SuccessFail s = new SuccessFail();
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
    public SuccessFail canGetWriteLock(Transaction t,String variable){
        SuccessFail s = lm.getWriteLock(t,variable);
        return s;
    }
    public SuccessFail writedata(Transaction t,String variable,int value){
        SuccessFail s = new SuccessFail();
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
    public Map<String,Integer> getDB(){
        return dm.getDB();
    }
}
