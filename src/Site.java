import java.util.*;

public class Site {
    public enum SiteStatus{ACTIVE,FAIL,RECOVER}

    DataManager dm;
    LockManager lm;
    private final int siteNo;
    public SiteStatus status;
    public int activeVarCount;

    public Site(int siteNo){
        dm = new DataManager();
        lm = new LockManager();
        this.siteNo = siteNo;
        status = SiteStatus.ACTIVE;
        activeVarCount = 20;
        initialWrite();
    }

    // Checks whether the transaction can get a lock for the given variable.
    // It returns the data if the transaction can get a lock or else it returns false.
    // Two cases where it cannot get a lock- 
    // Either the site is down or else some other transaction holds the lock.
    public Result readdata(String t,String variable){
        Result s = new Result(false,-1,"");
        int variableNo = Integer.parseInt(variable.substring(1));
        if(status == SiteStatus.ACTIVE || (siteNo == 1+variableNo%10 && status == SiteStatus.RECOVER)){
            s = lm.getReadLock(t,variable);
            if(s.status && dm.getRecoveryStatus(variable)){
                s.value = dm.readCommitData(variable);
            }
        }
        else{
            s.status = false;
        }
        return s;
    }

    // Checks whether the transaction can get a lock for the given variable.
    public Result canGetWriteLock(String t,String variable){
        return lm.getWriteLock(t,variable);
    }

    // Writes data to the database for this site using the datamanager.
    // Returns False if the site is down.
    public Result writedata(String t,String variable,int value){
        Result s = new Result(false,-1,"");
        if(!status.equals(SiteStatus.FAIL)){
            // status = SiteStatus.ACTIVE;
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

    // Change the status of the Site.
    public void changeStatus(SiteStatus status){
        this.status = status;
    }

    // Returns the status of the Site.
    public SiteStatus getStatus(){
        return status;
    }

    // Returns the copy of the database
    public Map<String,Integer> getDB(){
        return dm.getDB();
    }

    // Removes locks for the given variable held by the given transaction
    // Returns whether the status of the site changed to ACTIVE.
    public boolean removeLock(String variable, String t,boolean commit,int time){
        // Commits data
        boolean statuschanged = false;
        if(commit){
            // Increases the count of variables accessed for write during the recovered stage.
            if(status.equals(SiteStatus.RECOVER)) activeVarCount += 1;
            // Changes the status of the site to ACTIVE after every variable is accessed for a write.
            if(activeVarCount == 20){
                changeStatus(SiteStatus.ACTIVE);
                statuschanged = true;
            }
            dm.commit(variable, time);
        }
        lm.removeLock(variable, t);
        return statuschanged;
    }

    // Used to find data closest to the given time.
    public List<Integer> finddata(String variable,int time){
        return dm.findData(variable, time);
    }

    // Change status of transactions that hold locks at this site to "TO_BE_ABORTED".
    public void tobeaborted(Map<String,Transaction> transactions){
        lm.tobeaborted(transactions);
    }

    // public ArrayList<Integer> getCommitTime(String variable){
    //     return dm.getcommitTime(variable);
    // }

    // Returns committed data.
    public int getCommit(String variable){
        return dm.readCommitData(variable);
    }

    // Changes status of variable at this site during fail.
    public void changeVarRecoveryStatus(){
        activeVarCount = 0;
        dm.changeVarRecoveryStatus();
    }

    // Returns the active variables count at this site.
    public int getActiveVarCount(){
        return activeVarCount;
    }

    // Initial write at the start of the site.
    public void initialWrite(){
        for(int i = 1;i<=20;i++){
            if(i%2 == 1 && siteNo == 1+i%10){
                String variable = "x".concat(Integer.toString(i));
                dm.writeData(variable, 10*(i));
                dm.commit(variable, 0);
            }
            else if(i%2 == 0){
                String variable = "x".concat(Integer.toString(i));
                dm.writeData(variable, 10*(i));
                dm.commit(variable, 0);
            }
        }
    }
}
