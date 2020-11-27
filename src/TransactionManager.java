import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.Map;
import java.util.List;

public class TransactionManager {

    // Operations class which stores information about the operation
    private class Operation{
        public String opType;
        public int value;
        public String transaction;
        public Operation(String opType,int value,String transaction){
            this.opType = opType;
            this.value = value;
            this.transaction = transaction;
        }
    }

    private Site[] sites;
    private int time;
    private int variables;
    private HashMap<String,Transaction> transactions;
    private HashMap<String,Set<String>> waitsForGraph;
    private HashMap<String,ArrayList<Operation>> waitOperations;
    private HashMap<Integer,List<Integer>> SiteFailHistory;

    public void initialData(){
        for(int i = 1;i<=20;i++){
            if(i%2 == 1){
                if(!sites[(i%10)].dm.writeData("x".concat(Integer.toString(i)), 10*(i))){
                    System.out.println("Couldn't write Data");
                }
            }
            else{
                for(int j = 0;j<this.sites.length;j++){
                    if(!sites[j].dm.writeData("x".concat(Integer.toString(i)), 10*(i))){
                        System.out.println("Couldn't write Data");
                    }
                }
            }
        }
    }
    TransactionManager(int siteCount,int variables){
        this.sites = new Site[siteCount];
        this.variables = variables;
        transactions = new HashMap<>();
        waitsForGraph = new HashMap<>();
        time = 0;
    }
    public SuccessFail readRequest(String transaction, String variable){
        Transaction t = transactions.get(transaction);
        t.addOperation("Read",variable);
        SuccessFail result = new SuccessFail(false,0,transaction);
        if(waitOperations.get(variable) != null){
            waitOperations.get(variable).add(new Operation("R", -1,transaction));
            waitsForGraph.get(transaction).add(result.transaction);
            return result;
        }
        if(t.isReadOnly()){
            // if(sites[t.getSite()].getStatus().equals(Site.SiteStatus.FAIL))
            for(int siteNo =0; siteNo<10;siteNo++){
                if(sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE)){
                    List<Integer> temp = sites[siteNo].finddata(variable, t.getStartTime());
                    List<Integer> tempFailList = SiteFailHistory.get(siteNo);
                    if(tempFailList.get(tempFailList.size()-1) < temp.get(0)){
                        continue;
                    }
                    int low = 0;
                    int high = temp.size()-1;
                    int mid;
                    while(low<high){
                        mid = (int)(low+high)/2;
                        if(tempFailList.get(mid) >= time){
                            high = mid;
                        }
                        else{
                            low = mid+1;
                        }
                    }
                    if(tempFailList.get(low) > time){
                        result.value = temp.get(1);
                        result.status = true;
                        return result;
                    }
                }
            }
            return result;
        }
        else{
            int siteNo = -1;
            int variableNo = Integer.parseInt(variable.substring(1, variable.length()-1));
            if(variableNo%2 == 1){
                siteNo = (variableNo%10);
                result = sites[siteNo].readdata(t,variable);
            }
            else{
                do{
                    siteNo += 1;
                    result = sites[siteNo].readdata(t,variable);
                }while(siteNo < 10 && !result.status && !sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE));
            }
            if(!result.status){
                if(siteNo < 10 && sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE)){
                    if(waitsForGraph.get(transaction) == null)
                        waitsForGraph.put(transaction,new HashSet<>());
                    waitsForGraph.get(transaction).add(result.transaction);
                }
                if(waitOperations.get(variable) == null)
                    waitOperations.put(variable,new ArrayList<>());
                    // deadlock check;
                waitOperations.get(variable).add(new Operation("R",-1,transaction));
            }
            if(result.status) transactions.get(transaction).addToLocktable(variable, "R");
            return result;
        }
    }

    public SuccessFail writeRequest(String transaction, String variable, int value){
        Transaction t = transactions.get(transaction);
        t.addOperation("Write",variable);
        SuccessFail result = new SuccessFail(false,value,transaction);
        if(waitOperations.get(variable) != null){
            waitOperations.get(variable).add(new Operation("W", value,transaction));
            waitsForGraph.get(transaction).add(result.transaction);
            return result;
        }
        int siteNo = 0;
        boolean islocked = false;
        boolean isactive = false;
        int variableNo = Integer.parseInt(variable.substring(1, variable.length()-1));
        if(variableNo%2 == 1){
            siteNo = (variableNo%10);
            result = sites[siteNo].writedata(t,variable,value);
            if(sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE)){
                if(!result.status){
                    islocked = true;
                }
                isactive = true;
            }
        }
        else{
            while(siteNo<10){
                result = sites[siteNo].canGetWriteLock(t, variable);
                if(sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE)){
                    if(!result.status){
                        islocked = true;
                        break;
                    }
                    isactive = true;
                }
                siteNo ++;
            }
            // if(siteNo != 10) return result;

            int siteNoTemp = 0;
            if(!islocked && isactive && siteNo == 10){
                while(siteNoTemp<10){
                    if(sites[siteNoTemp].getStatus().equals(Site.SiteStatus.ACTIVE)){
                        result = sites[siteNoTemp].writedata(t, variable, value);
                    }
                    siteNoTemp ++;
                }
            }

        }
        if(!result.status){
            result.status = false;
            if(waitOperations.get(variable) == null)
                waitOperations.put(variable,new ArrayList<>());
            waitOperations.get(variable).add(new Operation("W",value,transaction));
        }

        if(islocked){
            result.status = false;
            if(waitsForGraph.get(transaction) == null)
                waitsForGraph.put(transaction,new HashSet<>());
            waitsForGraph.get(transaction).add(result.transaction);

        }
        if(result.status) transactions.get(transaction).addToLocktable(variable, "W");
        return result;
    }

    public void beginTransaction(String transaction){
        time += 1;
        Transaction t = new Transaction(transaction, false, Status.ACTIVE, time);
        transactions.put(transaction,t);
    }
    public void beginROTransaction(String transaction){
        time += 1;
        int siteNo = -1;
        Transaction t = new Transaction(transaction, true, Status.ACTIVE, time);
        // do{
        //     siteNo += 1;
        // }while(siteNo < 10 && !sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE));
        // if(siteNo<10){
        //     t.addToSnapshot(sites[siteNo].getDB(),siteNo);
        // }
        // abort transaction if all sites down i.e siteNo = 10
        transactions.put(transaction,t);
    }

    private void release_locks(String transaction) {
        HashMap<String,String> locktable = transactions.get(transaction).getLocktable();
        for(Map.Entry<String,String> lock: locktable.entrySet()) {
            String variable = lock.getKey();
            // String locktype = lock.getValue();
            for(int siteNo = 0; siteNo<10; siteNo++){
                sites[siteNo].removeLock(variable, transactions.get(transaction));
            }
            boolean check = true;
            SuccessFail result = new SuccessFail(false, -1, transaction);
            while(check){
                if(waitOperations.get(variable).isEmpty()){
                    waitOperations.remove(variable);
                    break;
                }
                Operation nextOperation = waitOperations.get(variable).remove(0);
                
                if(nextOperation.opType == "R"){
                    result = readRequest(nextOperation.transaction, variable);
                }
                else if(nextOperation.opType == "W"){
                    result = writeRequest(nextOperation.transaction, variable, nextOperation.value);
                }
                check = result.status;
            }
        }
    }

    public void end(String transaction){
        if(transactions.get(transaction).getStatus().equals(Status.ACTIVE)){
            commit(transaction);
        }
    }

    public void commit(String transaction){
        
    }
    
    public void abort(String transaction) {
        transactions.get(transaction).setStatus(Status.ABORTED);
        waitsForGraph.remove(transaction);
        for(Map.Entry<String,Set<String>> mapElement : waitsForGraph.entrySet()) { 
            mapElement.getValue().remove(transaction);
        }
       release_locks(transaction);
    }

    private SuccessFail dfs(String u, HashSet<String> visited, HashSet<String> recursion_stack, String youngest_transaction) {
        visited.add(u);
        recursion_stack.add(u);
        if(transactions.get(u).getStartTime() > transactions.get(youngest_transaction).getStartTime()) youngest_transaction = u;
        for(String v : waitsForGraph.get(u)) {
            if(!visited.contains(v)) {
                SuccessFail cycle_result = dfs(v,visited,recursion_stack,youngest_transaction);
                if(cycle_result.status) return cycle_result;
            } 
            else if(recursion_stack.contains(v)) return new SuccessFail(true,-1,youngest_transaction);
        }
        recursion_stack.remove(u);
        return new SuccessFail(false, -1, youngest_transaction);
    }

    private SuccessFail check_deadlock(){
        HashSet<String> visited = new HashSet<>();
        HashSet<String> recursion_stack = new HashSet<>();
        for(Map.Entry<String,Set<String>> mapElement : waitsForGraph.entrySet()) { 
            String u = (String)mapElement.getKey(); 
            if(!visited.contains(u)) {
                SuccessFail cycle_result = dfs(u, visited,recursion_stack, u);
                if(cycle_result.status) return cycle_result;
            } 
        } 
        return new SuccessFail(false, -1, "");
    }


    public void fail(int site){
        time ++;
        SiteFailHistory.get(site).add(time);
        sites[site].abortornot(transactions);
        transactions.forEach((K,T)->{
            if(T.getStatus().equals(Status.TO_BE_ABORTED)){
                abort(T.getName());
            }
        });
        sites[site-1].changeStatus(Site.SiteStatus.FAIL);
        // all transactions that accessed this site to be aborted
    }
    public void recover(int site){
        time ++;
        sites[site-1].changeStatus(Site.SiteStatus.RECOVER);
    }
    public int getVar(){
        return variables;
    }
    public Site getSite(int siteno){
        return sites[siteno];
    }
}
