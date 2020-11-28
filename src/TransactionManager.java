import java.util.*;

public class TransactionManager {

    // Operations class which stores information about the operation
    private class Operation{
        public String opType;
        public int value;
        public String transaction;
        public String variable;

        public Operation(String opType,int value,String transaction, String variable){
            this.opType = opType;
            this.value = value;
            this.transaction = transaction;
            this.variable = variable;
        }
    }

    private final Site[] sites;
    private int time;
    private final int variables;
    private final HashMap<String,Transaction> transactions;
    private final HashMap<String,Set<String>> waitsForGraph;
    private final HashMap<String,ArrayList<Operation>> lockWaitOperations;
    private final ArrayList<Operation> failWaitOperations;
    private final HashMap<Integer,List<Integer>> SiteFailHistory;

    TransactionManager(int siteCount,int variables){
        this.sites = new Site[siteCount];
        for(int i=0;i<siteCount;i++){
            sites[i] = new Site(i+1);
        }
        this.variables = variables;
        transactions = new HashMap<>();
        waitsForGraph = new HashMap<>();
        lockWaitOperations = new HashMap<>();
        failWaitOperations = new ArrayList<>();
        SiteFailHistory = new HashMap<>();
        time = 0;
    }

    public Result snapshotResult(Transaction t, String variable,int siteNo){
        Result result = new Result(false,0,t.getName());
        if(sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE)){
            List<Integer> time_data = sites[siteNo].finddata(variable, t.getStartTime());
            List<Integer> tempFailList = SiteFailHistory.get(siteNo);

            if(!SiteFailHistory.containsKey(siteNo) || tempFailList.get(tempFailList.size()-1) < time_data.get(0)){
                result.value = time_data.get(1);
                result.status = true;
                return result;
            }

            int low = 0;
            int high = time_data.size()-1;
            int mid;
            while(low<high){
                mid = (low+high) /2;

                if(tempFailList.get(mid) >= t.getStartTime()){
                    high = mid;
                }
                else{
                    low = mid+1;
                }
            }

            if(tempFailList.get(low-1) < time_data.get(0)){
                result.value = time_data.get(1);
                result.status = true;
                return result;
            }
        }
        return result;
    }

    public Result readRequest(String transaction, String variable){
        Transaction t = transactions.get(transaction);
        Result result = new Result(false,0,transaction);
        
        if(t.isReadOnly()){
            int variableNo = Integer.parseInt(variable.substring(1));
            if(variableNo%2 == 1){
                result = snapshotResult(t, variable, variableNo%10);
            }
            else{
                for(int siteNo = 0; siteNo<10;siteNo++){
                    result = snapshotResult(t, variable, siteNo);
                    if(result.status){
                        break;
                    }
                }
            }
            if(!result.status) failWaitOperations.add(new Operation("R",-1,transaction, variable));
            return result;
        }
        else{
            boolean isactive = false;
            int siteNo = -1;
            int variableNo = Integer.parseInt(variable.substring(1));
            if(variableNo%2 == 1){
                siteNo = (variableNo%10);
                result = sites[siteNo].readdata(transaction,variable);
                if(!sites[siteNo].getStatus().equals(Site.SiteStatus.FAIL)) isactive = true;
            }
            else{
                do{
                    siteNo += 1;
                    result = sites[siteNo].readdata(transaction,variable);
                    if(sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE)) isactive = true;
                }while(siteNo < 9 && !result.status && !sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE));
            }

            if(result.status) {
                System.out.println(variable+": "+ result.value);
                transactions.get(transaction).addToLocktable(variable, "R");
            }
            else {
                if(!isactive) {
                    failWaitOperations.add(new Operation("R",-1,transaction, variable));    
                }
                else{
                    if(lockWaitOperations.get(variable) == null)
                        lockWaitOperations.put(variable,new ArrayList<>());
                    lockWaitOperations.get(variable).add(new Operation("R",-1,transaction, variable));

                    if(waitsForGraph.get(transaction) == null)
                        waitsForGraph.put(transaction,new HashSet<>());
                    waitsForGraph.get(transaction).add(result.transaction);
                    Result cycle_result = check_deadlock();
                    if(cycle_result.status){
                        abort_commit(cycle_result.transaction,true);
                    }
                }
            }

            return result;
        }
    }

    public Result writeRequest(String transaction, String variable, int value){
        Transaction t = transactions.get(transaction);
        Result result = new Result(false,value,transaction);
       
        int siteNo = 0;
        boolean islocked = false;
        boolean isactive = false;

        int variableNo = Integer.parseInt(variable.substring(1));
        if(variableNo%2 == 1){
            siteNo = (variableNo%10);
            result = sites[siteNo].writedata(transaction,variable,value);
            if(sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE)){
                isactive = true;
                if(!result.status){
                    islocked = true;
                }
            }
        }
        else{
            while(siteNo<10){
                result = sites[siteNo].canGetWriteLock(transaction, variable);
                if(sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE) || sites[siteNo].getStatus().equals(Site.SiteStatus.RECOVER)){
                    isactive = true;
                    if(!result.status){
                        islocked = true;
                        break;
                    }
                }
                siteNo++;
            }

            siteNo = 0;
            while(!islocked && isactive && siteNo<10){
                if(sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE) || sites[siteNo].getStatus().equals(Site.SiteStatus.RECOVER)){
                    result = sites[siteNo].writedata(transaction, variable, value);
                }
                siteNo++;
            }
        }

        if(result.status) transactions.get(transaction).addToLocktable(variable, "W");
        else if(!isactive) failWaitOperations.add(new Operation("W",value,transaction, variable));
        else if(islocked) {

            //Add to lockWait table
            if(lockWaitOperations.get(variable) == null)
            lockWaitOperations.put(variable,new ArrayList<>());
            lockWaitOperations.get(variable).add(new Operation("W",value,transaction, variable));

            //Add to waitsforgraph and check/remove deadlock
            result.status = false;
            if(waitsForGraph.get(transaction) == null)
                waitsForGraph.put(transaction,new HashSet<>());
            waitsForGraph.get(transaction).add(result.transaction);
            Result cycle_result = check_deadlock();
            if(cycle_result.status){
                abort_commit(cycle_result.transaction,true);
            }

        }

        return result;
    }

    public void beginTransaction(String transaction){
        time += 1;
        Transaction t = new Transaction(transaction, false, Status.ACTIVE, time);
        transactions.put(transaction,t);
    }
    public void beginROTransaction(String transaction){
        time += 1;
        Transaction t = new Transaction(transaction, true, Status.ACTIVE, time);
        transactions.put(transaction,t);
    }

    private void release_locks(String transaction,boolean commit) {
        HashMap<String,String> locktable = transactions.get(transaction).getLocktable();
        for(Map.Entry<String,String> lock: locktable.entrySet()) {
            String variable = lock.getKey();
            int variablenumber = Integer.parseInt(variable.substring(1));

            if(variablenumber%2 == 1) {
                sites[variablenumber%10].removeLock(variable, transaction, commit, time);
            }
            else {
                for(int siteNo = 0; siteNo<10; siteNo++){
                    sites[siteNo].removeLock(variable, transaction,commit,time);
                }
            }
            
            boolean check = true;
            Result result = new Result(false, -1, transaction);
            while(lockWaitOperations.containsKey(variable) && check){
                
                Operation nextOperation = lockWaitOperations.get(variable).get(0);

                if(nextOperation.opType == "R"){
                    result = readRequest(nextOperation.transaction, variable);
                }
                else if(nextOperation.opType == "W"){
                    System.out.println("NEXT TRANSACTION:"+nextOperation.transaction);
                    result = writeRequest(nextOperation.transaction, variable, nextOperation.value);
                }

                if(result.status) lockWaitOperations.get(variable).remove(0);
                if(lockWaitOperations.get(variable).isEmpty()) lockWaitOperations.remove(variable);

                check = result.status;
            }
        }
        // System.out.println("Release Locks");
        // lockWaitOperations.forEach((k,v)->{
        //     v.forEach((oper)->{
        //         System.out.println(oper.transaction+" "+oper.opType);
        //     });
        // });
    }

    public void endTransaction(String transaction){
        time++;
        List<Operation> waitingOperations = new ArrayList<>();
        lockWaitOperations.forEach((variable,operations) -> {
            operations.forEach((operation) -> {
                if(operation.transaction.equals(transaction)){
                    waitingOperations.add(operation);
                }
            });
        });

        waitingOperations.forEach((operation) ->{
            Result result;
            if(operation.opType == "W"){
                result = writeRequest(operation.transaction, operation.variable, operation.value);
                if(!result.status){
                    transactions.get(transaction).setStatus(Status.TO_BE_ABORTED);
                }
            }
            if(operation.opType == "R"){
                result = readRequest(operation.transaction, operation.variable);
                if(!result.status){
                    transactions.get(transaction).setStatus(Status.TO_BE_ABORTED);
                }
            }
        });

        if(transactions.get(transaction).getStatus().equals(Status.ACTIVE)){
            abort_commit(transaction, false);
        }
        else if(transactions.get(transaction).getStatus().equals(Status.TO_BE_ABORTED)){
            abort_commit(transaction, true);
        }
    }

    public void abort_commit(String transaction,boolean abort){
        if(abort){
            transactions.get(transaction).setStatus(Status.ABORTED);
            System.out.println("ABORT "+transaction);
        }
        else{
            transactions.get(transaction).setStatus(Status.COMMITTED);
            System.out.println("COMMIT "+transaction);
        }
        //Remove transaction from waitsforgraph
        waitsForGraph.remove(transaction);
        for(Map.Entry<String,Set<String>> mapElement : waitsForGraph.entrySet()) { 
            mapElement.getValue().remove(transaction);
        }

        //Remove transaction from lockWaitOperations
        for(Map.Entry<String,ArrayList<Operation>> mapElement : lockWaitOperations.entrySet()) { 
            mapElement.getValue().removeIf(operation -> operation.transaction.equals(transaction));
        }
        lockWaitOperations.entrySet().removeIf(entry -> lockWaitOperations.get(entry.getKey()).isEmpty()); 

       release_locks(transaction,!abort);

    }

    private Result dfs(String u, HashSet<String> visited, HashSet<String> recursion_stack, String youngest_transaction) {
        visited.add(u);
        recursion_stack.add(u);
        if(transactions.get(u).getStartTime() > transactions.get(youngest_transaction).getStartTime()) youngest_transaction = u;
        for(String v : waitsForGraph.getOrDefault(u,  Collections.emptySet())) {
            if(!visited.contains(v)) {
                Result cycle_result = dfs(v,visited,recursion_stack,youngest_transaction);
                if(cycle_result.status) return cycle_result;
            } 
            else if(recursion_stack.contains(v)) return new Result(true,-1,youngest_transaction);
        }
        recursion_stack.remove(u);
        return new Result(false, -1, youngest_transaction);
    }

    private Result check_deadlock(){
        HashSet<String> visited = new HashSet<>();
        HashSet<String> recursion_stack = new HashSet<>();
        for(Map.Entry<String,Set<String>> mapElement : waitsForGraph.entrySet()) { 
            String u = mapElement.getKey();
            if(!visited.contains(u)) {
                Result cycle_result = dfs(u, visited,recursion_stack, u);
                if(cycle_result.status) return cycle_result;
            } 
        } 
        return new Result(false, -1, "");
    }

    public void fail(int site){
        time++;
        if(!SiteFailHistory.containsKey(site)){
            SiteFailHistory.put(site, new ArrayList<>());
        }
        SiteFailHistory.get(site).add(time);
        sites[site-1].abortornot(transactions);
        sites[site-1].changeStatus(Site.SiteStatus.FAIL);
    }

    public void recover(int site){
        time++;
        sites[site-1].changeStatus(Site.SiteStatus.RECOVER);
        
        Iterator<Operation> itr = failWaitOperations.iterator(); 
        while (itr.hasNext()) { 
            Operation operation = itr.next(); 
            int variableNo = Integer.parseInt(operation.variable.substring(1));
            if (variableNo%2 == 0 || variableNo%10 + 1 == site) { 
                if(operation.opType == "R") 
                    readRequest(operation.transaction, operation.variable);
                else if(operation.opType == "W") 
                    writeRequest(operation.transaction, operation.variable, operation.value);
                itr.remove(); 
            } 
        }
    }

    public void dump(){
        for(int siteNo=0; siteNo<10;siteNo++){
            System.out.print("site " + ((siteNo + 1)) +" - ");
            for(int varNo = 1;varNo<=20;varNo++){
                String variable = "x".concat(Integer.toString(varNo));
                if(varNo%2 == 1 && siteNo != varNo%10){
                    continue;
                }
                System.out.print(variable+": "+ sites[siteNo].getCommit(variable) +", ");
            }
            System.out.println();
        }
    }
}
