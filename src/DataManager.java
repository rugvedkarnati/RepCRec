import java.util.HashMap;
import java.util.Map;

public class DataManager {
    private class Data{ 
        private int commitData; 
        private int currentData; 
        public Data(int commitData, int currentData) { 
          this.commitData = commitData; 
          this.currentData = currentData; 
        }
    }
    HashMap<String,Data> db;
    public DataManager(){
        db = new HashMap<>();
    }
    public int readData(String variable){
        return db.get(variable).currentData;
    }
    public int readCommitData(String variable){
        return db.get(variable).commitData;
    }
    public boolean writeData(String variable,int value){
        try{
            int cData = db.get(variable).commitData;
            db.put(variable,new Data(cData,value));
            return true;
        }
        catch(Exception e){
            return false;
        }
    }
    public boolean commit(String variable){
        int val = db.get(variable).currentData;
        db.get(variable).commitData = val;
        return true;
    }
    public Map<String,Integer> getDB(){
        HashMap<String,Integer> tempDB = new HashMap<>();
        db.forEach((variable,data) -> {
            tempDB.put(variable, data.commitData);
        });
        return tempDB;
    }
}
