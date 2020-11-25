import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DataManager {
    HashMap<String,List<Integer>> db;
    public DataManager(){
        db = new HashMap<>();
    }
    public int readData(String variable){
        return db.get(variable).get(1);
    }
    public int readCommitData(String variable){
        return db.get(variable).get(0);
    }
    public boolean writeData(String variable,int value){
        List<Integer> tup = new ArrayList<Integer>();
        try{
            tup.add(null);
            tup.add(value);
            db.put(variable,tup);
            return true;
        }
        catch(Exception e){
            return false;
        }
    }
    public boolean commit(String variable){
        int val = db.get(variable).get(1);
        db.get(variable).set(0, val);
        return true;
    }
    public void revertToCommit(String variable){
        int val = db.get(variable).get(0);
        db.get(variable).set(1, val);
    }
}
