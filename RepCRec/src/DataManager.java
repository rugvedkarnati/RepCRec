import java.util.*;

public class DataManager {

    // This class is used to store the tuple of commitData and currentData.
    private class Data{ 
        private int commitData; 
        private final int currentData;
        private final ArrayList<List<Integer>> commitHistory;

        // Status of the variable.
        private boolean isRecovered;
        public Data(int commitData, int currentData, ArrayList<List<Integer>> commitHistory) { 
          this.commitData = commitData; 
          this.currentData = currentData;
          this.commitHistory= commitHistory;
          isRecovered = true;
        }
    }

    // Database
    HashMap<String,Data> db;

    public DataManager(){
        db = new HashMap<>();
        for(int i = 1;i<=20;i++){
            db.put("x".concat(Integer.toString(i)),new Data(0,0,new ArrayList<>()));
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
        int cData = db.get(variable).commitData;
        ArrayList<List<Integer>> commitHistory = db.get(variable).commitHistory;
        db.put(variable,new Data(cData,value,commitHistory));
        return true;
    }

    // Commit puts the current value of the given variable into commitData for that given variable
    public boolean commit(String variable,int commitTime){
        int val = db.get(variable).currentData;
        db.get(variable).commitData = val;

        // Changes the status of the variable to recovered after a commit.
        db.get(variable).isRecovered = true;
        List<Integer> tempList = new ArrayList<>();
        tempList.add(commitTime);
        tempList.add(val);
        db.get(variable).commitHistory.add(tempList);
        return true;
    }

    // Retruns the recovery status of the variable.
    public boolean getRecoveryStatus(String variable){
        return db.get(variable).isRecovered;
    }

    // Changes the recovery status of all the variables to false during site fail.
    public void changeVarRecoveryStatus(){
        db.forEach((variable,data) ->{
            data.isRecovered = false;
        });
    }

    // Returns a copy of the HashMap
    public Map<String,Integer> getDB(){
        HashMap<String,Integer> tempDB = new HashMap<>();
        db.forEach((variable,data) -> 
            tempDB.put(variable, data.commitData)
        );
        return tempDB;
    }

    // Returns the data committed at a time closest to the given time.
    // Uses binary search technique to find the closest smaller value.
    public List<Integer> findData(String variable,int time){
        ArrayList<List<Integer>> time_data_list = db.get(variable).commitHistory;
        if(time_data_list.get(time_data_list.size()-1).get(0) <= time){
            return time_data_list.get(time_data_list.size()-1);
        }
        int low = 0;
        int high = time_data_list.size()-1;
        int mid;
        while(low<high){
            mid = (low+high) /2;
            if(time_data_list.get(mid).get(0) >= time){
                high = mid;
            }
            else{
                low = mid+1;
            }
        }
        if(time_data_list.get(low).get(0) == time) return time_data_list.get(low);
        else return time_data_list.get(low-1);
    }
}
