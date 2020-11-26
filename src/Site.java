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
    public void changeStatus(SiteStatus status){
        this.status = status;
    }
    public SiteStatus getStatus(){
        return status;
    }
}
