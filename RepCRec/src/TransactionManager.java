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
    private final HashMap<String,String> WriteWaiting;

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
        WriteWaiting = new HashMap<>();
        time = 0;
    }

    // Increases time after every input operation.
    public void addTime(){
        time += 1;
    }

    // This method gets the value that was committed just before the beginning of the readOnly transaction.
    // It also checks whether there was any fail site between that commit and the 
    // start of the readOnly transaction. 
    // We use concept of bisect_left(binary search) to find the closest value smaller than the given value.
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

    // This method is used to send a readRequest to the site for the given variable.
    // It resurns whehter it was able to get the locks on that variable and also the value.
    public Result readRequest(String transaction, String variable){
        Transaction t = transactions.get(transaction);
        Result result = new Result(false,0,transaction);
        if(WriteWaiting.getOrDefault(variable,null) != null){
            lockWaitOperations.get(variable).add(new Operation("R",-1,transaction, variable));
            //Add to waitsforgraph and check/remove deadlock
            result.status = false;
            if(waitsForGraph.get(transaction) == null)
                waitsForGraph.put(transaction,new HashSet<>());
            waitsForGraph.get(transaction).add(WriteWaiting.get(variable));
            Result cycle_result = check_deadlock();
            if(cycle_result.status){
                abort_commit(cycle_result.transaction,true);
            }
            return result;
        }
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
            else{
                System.out.println(variable+": "+ result.value);
            }
            return result;
        }
        else{
            boolean isactive = false;
            int siteNo = -1;
            int variableNo = Integer.parseInt(variable.substring(1));
            if(variableNo%2 == 1){
                siteNo = (variableNo%10);
                result = sites[siteNo].readdata(transaction,variable);
                if(!sites[siteNo].getStatus().equals(Site.SiteStatus.FAIL) && sites[siteNo].getRecoveryStatus(variable)) isactive = true;
            }
            else{
                do{
                    siteNo += 1;
                    result = sites[siteNo].readdata(transaction,variable);
                    if(sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE) && sites[siteNo].getRecoveryStatus(variable)) isactive = true;
                }while(siteNo < 9 && !result.status && !sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE));
            }

            if(result.status) {
                System.out.println(variable+": "+ result.value);
                transactions.get(transaction).addToLocktable(variable, "R");
            }
            else {
                // This check happens for fail due to down site.
                if(!isactive) {
                    failWaitOperations.add(new Operation("R",-1,transaction, variable));    
                }
                else{
                    // This check happens for fail due to unavailable lock on the variable.
                    if(lockWaitOperations.get(variable) == null)
                        lockWaitOperations.put(variable,new ArrayList<>());
                    lockWaitOperations.get(variable).add(new Operation("R",-1,transaction, variable));

                    if(waitsForGraph.get(transaction) == null)
                        waitsForGraph.put(transaction,new HashSet<>());
                    waitsForGraph.get(transaction).add(result.transaction);

                    // Check for cycle after every add to the graph.
                    Result cycle_result = check_deadlock();
                    if(cycle_result.status){
                        System.out.println("DeadLock Detected");
                        abort_commit(cycle_result.transaction,true);
                    }
                }
            }

            return result;
        }
    }

    // This method is used to send a write request to the available sites.
    // It returns whether it was succesfull in writing the value to the variable.
    public Result writeRequest(String transaction, String variable, int value){
        Transaction t = transactions.get(transaction);
        Result result = new Result(false,value,transaction);
        if(WriteWaiting.getOrDefault(variable,null) != null){
            lockWaitOperations.get(variable).add(new Operation("W",value,transaction, variable));
            //Add to waitsforgraph and check/remove deadlock
            result.status = false;
            if(waitsForGraph.get(transaction) == null)
                waitsForGraph.put(transaction,new HashSet<>());
            waitsForGraph.get(transaction).add(WriteWaiting.get(variable));
            Result cycle_result = check_deadlock();
            if(cycle_result.status){
                abort_commit(cycle_result.transaction,true);
            }
            return result;
        }
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
            // Checking whether the transaction can get a write lock from any up site.
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

            // Writing data to the available sites.
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
            // Adding to writeWaiting
            if(WriteWaiting.getOrDefault(variable,null) == null){
                WriteWaiting.put(variable, transaction);
            }
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
                System.out.println("DeadLock Detected");
                abort_commit(cycle_result.transaction,true);
            }

        }

        return result;
    }

    // Start a transaction
    public void beginTransaction(String transaction){
        Transaction t = new Transaction(transaction, false, Status.ACTIVE, time);
        transactions.put(transaction,t);
    }

    // Start a readOnly transaction
    public void beginROTransaction(String transaction){
        Transaction t = new Transaction(transaction, true, Status.ACTIVE, time);
        transactions.put(transaction,t);
    }

    // Executing Operations which were waiting for Fail
    public void executeFailWaitOperations(int siteNo){
        Iterator<Operation> itr = failWaitOperations.iterator();

        while (itr.hasNext()) { 
            Operation operation = itr.next(); 

            int variableNo = Integer.parseInt(operation.variable.substring(1));
            if (variableNo%2 == 0 || variableNo%10 + 1 == siteNo) { 
                itr.remove(); 
                if(operation.opType == "R"){ 
                    readRequest(operation.transaction, operation.variable);
                }
                else if(operation.opType == "W"){
                    writeRequest(operation.transaction, operation.variable, operation.value);
                }
            } 
        }
    }
    // Releases locks held by the transaction when it commits or aborts.
    // Executes all the operations waiting for the locks.
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
                    if(!sites[siteNo].getStatus().equals(Site.SiteStatus.FAIL)){
                        sites[siteNo].removeLock(variable, transaction,commit,time);
                    }
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
                    if(WriteWaiting.getOrDefault(variable, null) != null)
                        WriteWaiting.remove(variable);
                    result = writeRequest(nextOperation.transaction, variable, nextOperation.value);
                }

                if(result.status){
                    lockWaitOperations.get(variable).remove(0);
                }
                else{
                    lockWaitOperations.get(variable).remove(lockWaitOperations.get(variable).size()-1);
                }
                if(lockWaitOperations.getOrDefault(variable,null) != null && lockWaitOperations.get(variable).isEmpty()) lockWaitOperations.remove(variable);

                check = result.status;
            }
        }
    }

    // End a transaction
    // Execute any waiting operations of this transaction.
    public void endTransaction(String transaction){

        // Finding the waiting operations for this transaction.
        List<Operation> waitingOperations = new ArrayList<>();

        lockWaitOperations.forEach((variable,operations) -> {
            Iterator<Operation> itr = operations.iterator();

            while (itr.hasNext()) { 
                Operation operation = itr.next(); 
                if(operation.transaction.equals(transaction)){
                    waitingOperations.add(operation);
                    itr.remove();
                }
            }
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

    // Aborts or Commits a transaction.
    // Removes locks held by this transaction.
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

        HashSet<String> keys = new HashSet<>(WriteWaiting.keySet());
        keys.forEach((K) -> {
            if(WriteWaiting.get(K).equals(transaction)){
                WriteWaiting.remove(K);
            }
        });

        release_locks(transaction,!abort);

        // Executing failwait operations
        for(int siteNo=0;siteNo<10;siteNo++){
            executeFailWaitOperations(siteNo);
        }

    }

    // Depth First Search to find a cycle in the graph.
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

    // Deadlock detection.
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

    // Changes the status of all the transaction holding locks in this site to "TO_BE_ABORTED".
    public void fail(int site){
        sites[site-1].changeVarRecoveryStatus();
        sites[site-1].changeStatus(Site.SiteStatus.FAIL);
        // Add the status to SiteFailHistory.
        if(!SiteFailHistory.containsKey(site-1)){
            SiteFailHistory.put(site-1, new ArrayList<>());
        }
        SiteFailHistory.get(site-1).add(time);

        sites[site-1].tobeaborted(transactions);
        sites[site-1].removeSiteLocks();
    }

    // Recovery of a site.
    // Executes operations waiting for this site.
    public void recover(int site){
        sites[site-1].changeStatus(Site.SiteStatus.RECOVER);
        executeFailWaitOperations(site-1);
    }

    // Outputs gives the committed values of all copies of all variables at all sites, 
    // sorted per site with all values per site in ascending order by variable name.
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
