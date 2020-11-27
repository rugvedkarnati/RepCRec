import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DataManager {

    // This class is used to store the tuple of commitData and currentData.
    private class Data{ 
        private int commitData; 
        private int currentData;
        private ArrayList<List<Integer>> commitInfo;
        public Data(int commitData, int currentData) { 
          this.commitData = commitData; 
          this.currentData = currentData;
          commitInfo = new ArrayList<>();
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
    public boolean commit(String variable,int commitTime){
        int val = db.get(variable).currentData;
        db.get(variable).commitData = val;
        if(db.containsKey(variable)){
            List<Integer> tempList = new ArrayList<>();
            tempList.add(commitTime);
            tempList.add(val);
            db.get(variable).commitInfo.add(tempList);
        }
        else{
            db.get(variable).commitInfo = new ArrayList<>();
        }
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
    public List<Integer> findData(String variable,int time){
        ArrayList<List<Integer>> temp = db.get(variable).commitInfo;
        if(temp.get(temp.size()-1).get(0) < time){
            return temp.get(temp.size()-1);
        }
        int low = 0;
        int high = temp.size()-1;
        int mid;
        while(low<high){
            mid = (int)(low+high)/2;
            if(temp.get(mid).get(0) >= time){
                high = mid;
            }
            else{
                low = mid+1;
            }
        }
        return temp.get(low);
    }
    // public ArrayList<Integer> getcommitTime(String variable){
    //     return db.get(variable).commitInfo;
    // }
}
