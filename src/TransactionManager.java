import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class TransactionManager {
    private Site[] sites;
    private int time;
    private int variables;
    private HashMap<String,Transaction> transactions;
    private HashMap<String,ArrayList<Transaction>> waitsForGraph;
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
        int variableNo = Integer.parseInt(variable.substring(1, variable.length()-1));
        if(variableNo%2 == 1){
            return sites[(variableNo%10)].readdata(t,variable);
        }
        int siteNo = -1;
        SuccessFail result;
        do{
            siteNo += 1;
            result = sites[siteNo].readdata(t,variable);
        }while(siteNo < 10 && !result.status && !sites[siteNo].getStatus().equals(Site.SiteStatus.ACTIVE));
        if(siteNo == 10){
            result.status = false;
        }
        else{
            if(!result.status){
                
            }
            result.status = true;
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
    public void fail(int site){
        sites[site-1].changeStatus(Site.SiteStatus.FAIL);
    }
    public void recover(int site){
        sites[site-1].changeStatus(Site.SiteStatus.RECOVER);
    }
    public int getVar(){
        return variables;
    }
    public Site getSite(int siteno){
        return sites[siteno];
    }
}
