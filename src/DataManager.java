import java.util.HashMap;
import java.util.Map;

public class DataManager {

    // This class is used to store the tuple of commitData and currentData.
    private class Data{ 
        private int commitData; 
        private int currentData; 
        public Data(int commitData, int currentData) { 
          this.commitData = commitData; 
          this.currentData = currentData; 
        }
    }

    // Database
    HashMap<String,Data> db;

    public DataManager(){
        db = new HashMap<>();
    }

    // Reading Data from the HashMap
    public int readData(String variable){
        return db.get(variable).currentData;
    }

    // Reading Committed Data from the HashMap
    public int readCommitData(String variable){
        return db.get(variable).commitData;
    }

    // Writing Data to the HashMap
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

    // Commit puts the current value of the given variable into commitData for that given variable
    public boolean commit(String variable){
        int val = db.get(variable).currentData;
        db.get(variable).commitData = val;
        return true;
    }

    // Returns a copy of the HashMap
    public Map<String,Integer> getDB(){
        HashMap<String,Integer> tempDB = new HashMap<>();
        db.forEach((variable,data) -> 
            tempDB.put(variable, data.commitData)
        );
        return tempDB;
    }
}
