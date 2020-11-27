import java.util.Scanner;
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
            String command = sc.nextLine();
            if(command.compareTo("exit") == 0){
                sc.close();
                break;
            }
            System.out.println(command);
            String[] splitCommand = command.split("\\(");
            System.out.println(splitCommand);
            String s = "";
            SuccessFail result;
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
                    t.recover(Integer.parseInt(s.substring(0,s.length()-1)));
                    break;
                case "dump": //t.dump();
                    break;
                case "R": s = splitCommand[1];
                    String[] s1 = s.substring(0,s.length()-1).split(",");
                    result = t.readRequest(s1[0],s1[1]);
                    if(result.status){
                        System.out.println("SUCCESS");
                    }
                    else if(result.transaction.equals("")){
                        System.out.println("FAIL");
                    }
                    else{
                        System.out.println("LOCKS");
                    }
                    break;
                case "W": s = splitCommand[1];
                    String[] s2 = s.substring(0,s.length()-1).split(",");
                    result = t.writeRequest(s2[0],s2[1],Integer.parseInt(s2[2]));
                    if(result.status){
                        System.out.println("SUCCESS");
                    }
                    else if(result.transaction.equals("")){
                        System.out.println("FAIL");
                    }
                    else{
                        System.out.println("LOCKS");
                    }
                    break;
                default: break;
            }
        }
    }
}
