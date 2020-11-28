import java.util.Scanner;
import java.io.FileInputStream;
public class Main {
    public static void main(String[] args) throws Exception {
        TransactionManager t = new TransactionManager(10,20);
        FileInputStream file = null;
        Scanner sc;
        if(args.length > 0){
            file = new FileInputStream(args[0]);
            sc = new Scanner(file);
        }
        else{
            sc = new Scanner(System.in);
        }
        while(sc.hasNextLine()){
            String command = sc.nextLine().strip();
            System.out.println(command);
            String[] splitCommand = command.split("\\(");
            String s = "";
            Result result;
            switch(splitCommand[0]){
                case "begin": s = splitCommand[1].strip();
                    t.beginTransaction(s.substring(0,s.length()-1));
                    break;
                case "beginRO": s = splitCommand[1].strip();
                    t.beginROTransaction(s.substring(0,s.length()-1));
                    break;
                case "end": s = splitCommand[1].strip();
                    t.endTransaction(s.substring(0,s.length()-1));
                    break;
                case "fail": s = splitCommand[1].strip();
                    t.fail(Integer.parseInt(s.substring(0,s.length()-1)));
                    break;
                case "recover": s = splitCommand[1].strip();
                    t.recover(Integer.parseInt(s.substring(0,s.length()-1)));
                    break;
                case "dump": t.dump();
                    break;
                case "R": s = splitCommand[1].strip();
                    String[] s1 = s.substring(0,s.length()-1).split(",");
                    s1[0] = s1[0].strip();
                    s1[1] = s1[1].strip();
                    result = t.readRequest(s1[0],s1[1]);
                    if(result.status){
                        System.out.println("SUCCESS");
                        System.out.println(s1[1]+": "+Integer.toString(result.value));
                    }
                    else if(result.transaction.equals("")){
                        System.out.println("FAIL");
                    }
                    else{
                        System.out.println("LOCKS");
                    }
                    break;
                case "W": s = splitCommand[1].strip();
                    String[] s2 = s.substring(0,s.length()-1).split(",");
                    s2[0] = s2[0].strip();
                    s2[1] = s2[1].strip();
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
        sc.close();
    }
}
