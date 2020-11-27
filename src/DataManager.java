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
        for(int i = 1;i<=20;i++){
            db.put("x".concat(Integer.toString(i)),new Data(0,0));
        }
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
        // if(db.get(variable).commitInfo.isEmpty()){
        //     db.get(variable).commitInfo = new ArrayList<>();   
        // }
        System.out.println("DAMNN");
        List<Integer> tempList = new ArrayList<>();
        tempList.add(commitTime);
        tempList.add(val);
        db.get(variable).commitInfo.add(tempList);
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
        ArrayList<List<Integer>> time_data_list = db.get(variable).commitInfo;
        if(time_data_list.get(time_data_list.size()-1).get(0) < time){
            return time_data_list.get(time_data_list.size()-1);
        }
        int low = 0;
        int high = time_data_list.size()-1;
        int mid;
        while(low<high){
            mid = (int)(low+high)/2;
            if(time_data_list.get(mid).get(0) >= time){
                high = mid;
            }
            else{
                low = mid+1;
            }
        }
        return time_data_list.get(low);
    }
    // public int getcommitTime(String variable){
    //     return db.get(variable).commitInfo;
    // }
}
