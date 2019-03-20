import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Scanner;

public class Blatt01 {

    public static void main(String[] args)  throws Exception{
        int [] ref = {1,2,3};
        //************************************//
        try {
            Class.forName ( "org.postgresql.Driver" );
            System.err.println("Driver found.");
        } catch ( java.lang.ClassNotFoundException e ) {
            System.err.println("PostgreSQL JDBC Driver not found ... ");
            e.printStackTrace();
            return;
        }
        
        Scanner sc = new Scanner(System.in);
        
        String host = "biber.cosy.sbg.ac.at";
        String port = "5432";
        String database = "dbtuning_ss2019";
        System.out.println("enter password : ");
        String pwd = sc.nextLine();
        System.out.println("enter username : ");
        String user = sc.nextLine();
        String path = "auth.tsv";
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        
        Connection con = null;
        try {
            con = DriverManager.getConnection(url, user, pwd);
            System.err.println("Connection established.");
        } catch (Exception e) {
            System.err.println("Could not establish connection.");
            e.printStackTrace();
            return;
        }
        //************Iterate over different Approaches***************************//
        for(int a = 0; a < ref.length; a++) {
            
            String create_table = " CREATE TABLE auth(name varchar(49),pubID varchar(129));";
            con.createStatement().execute(create_table);
            
            if(ref[a] == 1) {
                
                System.out.println("Straightforward");
                BufferedReader reader = new BufferedReader(new FileReader(path));
                String line = reader.readLine();
                String values = "";
                String query = "";
                String[] lineArray;
                
                int count = 0;
                long start = System.currentTimeMillis();
                while (count < 30000) {
                    
                    values = "";
                    values += "('";
                    lineArray = line.split("\t");
                    
                    for (int i = 0; i < lineArray.length; i++) {
                            if (i < lineArray.length - 1) {
                                    values += lineArray[i].replace("'", "''") + "', '";
                            } else {
                                    values += lineArray[i].replace("'", "''") + "');";
                            }
                    }
                    
                    query = "INSERT INTO auth VALUES" + values;
                    
                    con.createStatement().execute(query);
                   
                    line = reader.readLine();
                    count++;  
                    
                }
                
                reader.close();
                
                long end = System.currentTimeMillis();
                long runtime = end - start;
                System.out.println("Runtime in milliseconds: " + runtime);
            }else if(ref[a] == 2){
                
                System.out.println("Single Query");
                long start = System.currentTimeMillis();
                try {
                        String qry = "INSERT INTO auth VALUES " + getValues(path) + ";";
                        System.out.println("Begin transmit");
                        con.createStatement().execute(qry);
                        System.out.println("Query sucessful.");
                } catch (Exception e) {
                        System.err.println("Query was not successful.");
                        e.printStackTrace();
                }
                long end = System.currentTimeMillis();
                long runtime = end - start;
                System.out.println("Runtime in milliseconds: " + runtime);
                
            }else if(ref[a] == 3){
                int counter = 0;
                System.out.println("Prepared Statement");
                long start = System.currentTimeMillis();
                BufferedReader reader = new BufferedReader(new FileReader(path));
                String line = reader.readLine();
                String query = "INSERT INTO auth values(?, ?);";
                String[] cols = new String[2];
                PreparedStatement ps = con.prepareStatement(query);
 
                while(counter < 30000){
                 
                   cols = line.split("\t");
                   ps.setString(1, cols[0]);
                   ps.setString(2, cols[1]);
                   ps.addBatch();
                   line = reader.readLine();
                   counter ++;
                  
              }
                
              System.out.println("Begin transmit");
              ps.executeBatch();
        
              reader.close(); 
              long end = System.currentTimeMillis();
              long runtime = end - start;
              System.out.println("Runtime in milliseconds: " + runtime);
            }
            
            String query = "DROP TABLE auth;";
            con.createStatement().execute(query);
            
        }
        
    }
    
   static String getValues(String path) throws Exception {

        StringBuilder dataBuilder = new StringBuilder();
        BufferedReader file = new BufferedReader(new FileReader(path));

        String dataRow = file.readLine();
        int count = 0;
        while (count < 30000) {
                dataRow = dataRow.replace("'", "''");
                dataRow = dataRow.replace("\t", "','");
                dataBuilder.append("('" + dataRow + "'),");
                dataRow = file.readLine(); 
                count++;
        }
        file.close();

        String data = dataBuilder.toString();
        data = data.substring(0, data.length() - 1);

        return data;
}
    
}
