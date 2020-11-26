import java.util.Scanner;
import java.io.File;
import java.io.FileInputStream;
public class Main {
    public static void main(String[] args) throws Exception {
        TransactionManager t = new TransactionManager(10,20);
        t.initialData();
        FileInputStream file = null;
        Scanner sc;
        if(args.length > 0){
            file = new FileInputStream(args[0]);
            sc = new Scanner(file);
        }
        else{
            sc = new Scanner(System.in);
        }
        while(true){
            if(sc.nextLine().compareTo("exit") == 0){
                break;
            }
            String command = sc.nextLine();
            String[] splitCommand = command.split("(");
            String s = "";
            switch(splitCommand[0]){
                case "begin": s = splitCommand[1];
                    t.beginTransaction(s.substring(0,s.length()-1));
                    break;
                case "beginRO": s = splitCommand[1];
                    t.beginROTransaction(s.substring(0,s.length()-1));
                    break;
                case "end": s = splitCommand[1];
                    t.endTransaction(s.substring(0,s.length()-1));
                    break;
                case "fail": s = splitCommand[1];
                    t.fail(Integer.parseInt(s.substring(0,s.length()-1)));
                    break;
                case "recover": s = splitCommand[1];
                    t.recover(s.substring(0,s.length()-1));
                    break;
                case "dump": t.dump();
                    break;
                case "R": s = splitCommand[1];
                    String[] s1 = s.substring(0,s.length()-1).split(",");
                    t.readRequest(s1[0],s1[1]);
                    break;
                case "W": s = splitCommand[1];
                    String[] s2 = s.substring(0,s.length()-1).split(",");
                    t.writeRequest(s2[0],s2[1],s2[2]);
                    break;
                default: break;
            }
        }
    }
}
