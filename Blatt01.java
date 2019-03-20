import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

public class Blatt01 {

    public static void main(String[] args)  throws Exception{
       
        try {
            Class.forName ( "org.postgresql.Driver" );
            System.err.println("Driver found.");
        } catch ( java.lang.ClassNotFoundException e ) {
            System.err.println("PostgreSQL JDBC Driver not found ... ");
            e.printStackTrace();
            return;
        }
        
        Scanner sc = new Scanner(System.in);
        
        System.out.println("enter 1 for straight forward \n" +
                           "enter 2 for single Query \n" +
                           "enter 3 for prepared statement");
        String input = sc.nextLine();
        
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
            
        String create_table = " CREATE TABLE auth(name varchar(49),pubID varchar(129));";
        con.createStatement().execute(create_table);
            
        long start = System.currentTimeMillis();
            
        if(input.equals("1")) {
            
            Straightforward(path,con);
            
        }else if(input.equals("2")){
               
            single_query(path,con);
                
        }else if(input.equals("3")){
              
            prepared_statement(path,con);
                
        }else {
            System.out.println("invalid input");
        }
        
        long end = System.currentTimeMillis();
        long runtime = end - start;
        System.out.println("Runtime in milliseconds: " + runtime);
        String query = "DROP TABLE auth;";
        con.createStatement().execute(query);
    }
    
   static String getValues(String path) throws Exception {

        StringBuilder dataBuilder = new StringBuilder();
        BufferedReader file = new BufferedReader(new FileReader(path));

        String dataRow = file.readLine();
        
        for (int count = 0; count < 30000; count++) {
                dataRow = dataRow.replace("'", "''");
                dataRow = dataRow.replace("\t", "','");
                dataBuilder.append("('" + dataRow + "'),");
                dataRow = file.readLine(); 
        }
        file.close();

        String data = dataBuilder.toString();
        data = data.substring(0, data.length() - 1);

        return data;
}
   
   static void Straightforward(String path,Connection con) throws IOException {
       System.out.println("Straightforward");
       BufferedReader reader = new BufferedReader(new FileReader(path));
       String line = reader.readLine();
       String values = "";
       String query = "";
       String[] lineArray;
       
       for (int count = 0; count < 30000; count++) {
           
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
           
           try {
            con.createStatement().execute(query);
           } catch (SQLException e) {
            System.err.println("Unable to create Statement");
            e.printStackTrace();
           }
          
           line = reader.readLine();
       }
       
       reader.close();
   }
   
   static void single_query(String path,Connection con) {
       System.out.println("Single Query");
       
       try {
               String qry = "INSERT INTO auth VALUES " + getValues(path) + ";";
               System.out.println("Begin transmit");
               con.createStatement().execute(qry);
               System.out.println("Query sucessful.");
       } catch (Exception e) {
               System.err.println("Query was not successful.");
               e.printStackTrace();
       }
   }
   
   static void prepared_statement(String path,Connection con) throws IOException, SQLException {
      
       System.out.println("Prepared Statement");
      
       BufferedReader reader = new BufferedReader(new FileReader(path));
       String line = reader.readLine();
       String query = "INSERT INTO auth values(?, ?);";
       String[] cols = new String[2];
       PreparedStatement ps = con.prepareStatement(query);

       for( int count = 0; count < 30000; count++){
          cols = line.split("\t");
          ps.setString(1, cols[0]);
          ps.setString(2, cols[1]);
          ps.addBatch();
          line = reader.readLine();
       }
       
       System.out.println("Begin transmit");
       ps.executeBatch();

       reader.close();
   }
}
