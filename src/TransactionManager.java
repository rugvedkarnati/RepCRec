public class TransactionManager {
    private Site[] sites;
    private int variables;
    public void initialData(){
        for(int i = 0;i<20;i++){
            if((i+1)%2 == 1){
                if(!sites[(i+1)%10].dm.writeData("x".concat(Integer.toString(i+1)), 10*(i+1))){
                    System.out.println("Couldn't write Data");
                }
            }
            else{
                for(int j = 0;j<this.sites.length;j++){
                    if(!sites[j].dm.writeData("x".concat(Integer.toString(i+1)), 10*(i+1))){
                        System.out.println("Couldn't write Data");
                    }
                }
            }
        }
    }
    TransactionManager(int sites,int variables){
        this.sites = new Site[sites];
        this.variables = variables;
    }
    public void beginTransaction(String transaction){

    }
    public int getVar(){
        return variables;
    }
    public Site getSite(int siteno){
        return sites[siteno];
    }
}
