/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queryrunner;

import java.util.ArrayList;
import java.util.Scanner;

/**
 *
 * QueryRunner takes a list of Queries that are initialized in it's constructor
 * and provides functions that will call the various functions in the QueryJDBC class
 * which will enable MYSQL queries to be executed. It also has functions to provide the
 * returned data from the Queries. Currently the eventHandlers in QueryFrame call these
 * functions in order to run the Queries.
 */
public class QueryRunner {


    public QueryRunner()
    {
        this.m_jdbcData = new QueryJDBC();
        m_updateAmount = 0;
        m_queryArray = new ArrayList<>();
        m_error="";


        this.m_projectTeamApplication="PARKING WARNING";

        // Each row that is added to m_queryArray is a separate query. It does not work on Stored procedure calls.
        // Please do not change Format for each row of m_queryArray is: (QueryText, ParamaterLabelArray[], LikeParameterArray[], IsItActionQuery, IsItParameterQuery)

        //    QueryText is a String that represents your query. It can be anything but Stored Procedure
        //    Parameter Label Array  (e.g. Put in null if there is no Parameters in your query, otherwise put in the Parameter Names)
        //    LikeParameter Array  is an array I regret having to add, but it is necessary to tell QueryRunner which parameter has a LIKE Clause. If you have no parameters, put in null. Otherwise put in false for parameters that don't use 'like' and true for ones that do.
        //    IsItActionQuery (e.g. Mark it true if it is, otherwise false)
        //    IsItParameterQuery (e.g.Mark it true if it is, otherwise false)

        // Query #1
        m_queryArray.add(new QueryData("Select * from owner", null, null, false, false));
        // Query #2
        m_queryArray.add(new QueryData("Select * from owner where owner_id=?", new String [] {"OWNER_ID"}, new boolean [] {false},  false, true));
        // Query #3
        m_queryArray.add(new QueryData("Select * from location where city like ?", new String [] {"CITY"}, new boolean [] {true}, false, true));
        // Query #4
        m_queryArray.add(new QueryData("insert into owner (owner_id, first_name, last_name, age, phone_number) values (?,?,?,?,?)",new String [] {"OWNER_ID", "FIRST_NAME", "LAST_NAME", "AGE", "PHONE_NUMBER"}, new boolean [] {false, false, false, false, false}, true, true));

        // Query #5
        m_queryArray.add(new QueryData("SELECT inc.location_id, loc.city, loc.state, COUNT(inc.location_id) AS Incidents\r\n" +
        		"FROM incident as inc, location as loc WHERE inc.location_id = loc.location_id\r\n" +
        		"GROUP BY inc.location_id, loc.city, loc.state ORDER BY Incidents DESC Limit 5;", null, null, false, false));

        // Query #6
        m_queryArray.add(new QueryData(
        		"SELECT  \r\n"
        		+ "SUM(CASE WHEN owner.age >=  0 \r\n"
        		+ "AND\r\n"
        		+ "owner.age < 35 THEN 1 ELSE 0 END)\r\n"
        		+ "AS 'age < 35',\r\n"
        		+ "SUM(CASE WHEN owner.age >=  35 \r\n"
        		+ "AND owner.age < 55 THEN 1 ELSE 0 END)\r\n"
        		+ "AS '35 < age < 55', \r\n"
        		+ "SUM(CASE WHEN owner.age >=  55 \r\n"
        		+ "AND owner.age < 75 THEN 1 ELSE 0 END)\r\n"
        		+ "AS '55 < age < 75',\r\n"
        		+ "SUM(CASE WHEN owner.age >= 75 THEN 1 ELSE 0 END)\r\n"
        		+ "AS '75 < age'\r\n"
        		+ "FROM incident\r\n"
        		+ "JOIN vehicle\r\n"
        		+ "ON incident.vehicle_id = vehicle.vehicle_id \r\n"
        		+ "JOIN owner\r\n"
        		+ "ON vehicle.owner_id = owner.owner_id \r\n"
        		+ "WHERE outcome_id = 1;\n" +
        		"", null, null, false, false));

        // Query #7
        m_queryArray.add(new QueryData(
        		"SELECT  loc.city AS 'city of citation',\r\n"
        		+ "loc.state AS 'state of citation',\r\n"
        		+ "o.city AS 'DL_City',  o.state AS 'DL_State'\r\n"
        		+ "FROM incident inc\r\n"
        		+ "JOIN location loc\r\n"
        		+ "ON loc.location_id = inc.location_id\r\n"
        		+ "JOIN vehicle veh\r\n"
        		+ "ON veh.vehicle_id = inc.vehicle_id\r\n"
        		+ "JOIN owner o\r\n"
        		+ "ON o.owner_id = veh.owner_id\r\n"
        		+ "JOIN license lic\r\n"
        		+ "ON o.owner_id = lic.owner_id\r\n"
        		+ "WHERE inc.outcome_id = 1;", null, null, false, false));

        // Query #8
        m_queryArray.add(new QueryData(
        		"SELECT p.mobile_carrier, m.mobile_carrier_name,\r\n"
        		+ "COUNT(m.mobile_carrier_name)\r\n"
        		+ "AS 'user adoption (ct)'\r\n"
        		+ "FROM phone p\r\n"
        		+ "JOIN mobile_carrier m\r\n"
        		+ "ON p.mobile_carrier = m.mobile_carrier_id\r\n"
        		+ "GROUP BY m.mobile_carrier_name, p.mobile_carrier\r\n"
        		+ "ORDER BY 'user adoption (ct)' DESC;"
        		, null, null, false, false));

     // Query #9
        m_queryArray.add(new QueryData(
        		"SELECT p.phone_brand, m.mobile_carrier_name,\n" +
        		"COUNT(m.mobile_carrier_name) -- AS [total mobile carrier users]\n" +
        		"FROM phone p\n" +
        		"JOIN mobile_carrier m ON p.mobile_carrier =\n" +
        		"m.mobile_carrier_id\n" +
        		"GROUP BY m.mobile_carrier_name, p.phone_brand\n" +
        		"ORDER BY p.phone_brand, m.mobile_carrier_name\n"
        		, null, null, false, false));

     // Query #10
        m_queryArray.add(new QueryData(
        		"SELECT\n" +
        		"SUM(CASE WHEN HOUR(datetime) BETWEEN 5 AND 12 THEN 1 ELSE 0\n" +
        		"END) AS Morning,\n" +
        		"SUM(CASE WHEN HOUR(datetime) BETWEEN 13 AND 17 THEN 1 ELSE 0 END)\n" +
        		"AS Afternoon,\n" +
        		"SUM(CASE WHEN HOUR(datetime) BETWEEN 18 AND 22 THEN 1 ELSE 0\n" +
        		"END) AS Evening,\n" +
        		"SUM(CASE WHEN HOUR(datetime) >= 23 OR HOUR(datetime) <= 4 THEN 1\n" +
        		"ELSE 0 END) AS Night\n" +
        		"FROM incident;\n"
        		, null, null, false, false));

     // Query #11  WE OUT HERE
        m_queryArray.add(new QueryData(
        		"SELECT\n" +
        		"SUM(CASE WHEN MONTH(datetime) = ?\n" +
        		"AND YEAR(datetime) = ? THEN 1 ELSE 0 END)\n" +
        		"AS 'Total Number of Warnings given in November 2016'\n" +
        		"FROM incident\n" +
        		"WHERE outcome_id = 0;\n"
        		, new String[] {"Month (Number)", "Year"}, new boolean [] {false, false}, false, true));

     // Query #12
        m_queryArray.add(new QueryData(
        		"SELECT\n" +
        		"SUM(i.outcome_id) AS 'Citation (sum)', c.mobile_carrier_name\n" +
        		"FROM incident i\n" +
        		"JOIN vehicle v\n" +
        		"ON i.vehicle_id = v.vehicle_id\n" +
        		"JOIN owner o\n" +
        		"ON v.owner_id = o.owner_id\n" +
        		"JOIN phone p\n" +
        		"ON o.owner_id = p.owner_id\n" +
        		"JOIN mobile_carrier c\n" +
        		"ON c.mobile_carrier_id = p.mobile_carrier\n" +
        		"WHERE i.outcome_id = 1\n" +
        		"GROUP BY c.mobile_carrier_id;\n"
        		, null, null, false, false));

    }


    public int GetTotalQueries()
    {
        return m_queryArray.size();
    }

    public int GetParameterAmtForQuery(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetParmAmount();
    }

    public String  GetParamText(int queryChoice, int parmnum )
    {
       QueryData e=m_queryArray.get(queryChoice);
       return e.GetParamText(parmnum);
    }

    public String GetQueryText(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.GetQueryString();
    }

    /**
     * Function will return how many rows were updated as a result
     * of the update query
     * @return Returns how many rows were updated
     */

    public int GetUpdateAmount()
    {
        return m_updateAmount;
    }

    /**
     * Function will return ALL of the Column Headers from the query
     * @return Returns array of column headers
     */
    public String [] GetQueryHeaders()
    {
        return m_jdbcData.GetHeaders();
    }

    /**
     * After the query has been run, all of the data has been captured into
     * a multi-dimensional string array which contains all the row's. For each
     * row it also has all the column data. It is in string format
     * @return multi-dimensional array of String data based on the resultset
     * from the query
     */
    public String[][] GetQueryData()
    {
        return m_jdbcData.GetData();
    }

    public String GetProjectTeamApplication()
    {
        return m_projectTeamApplication;
    }
    public boolean  isActionQuery (int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.IsQueryAction();
    }

    public boolean isParameterQuery(int queryChoice)
    {
        QueryData e=m_queryArray.get(queryChoice);
        return e.IsQueryParm();
    }


    public boolean ExecuteQuery(int queryChoice, String [] parms)
    {
        boolean bOK = true;
        QueryData e=m_queryArray.get(queryChoice);
        bOK = m_jdbcData.ExecuteQuery(e.GetQueryString(), parms, e.GetAllLikeParams());
        return bOK;
    }

     public boolean ExecuteUpdate(int queryChoice, String [] parms)
    {
        boolean bOK = true;
        QueryData e=m_queryArray.get(queryChoice);
        bOK = m_jdbcData.ExecuteUpdate(e.GetQueryString(), parms);
        m_updateAmount = m_jdbcData.GetUpdateCount();
        return bOK;
    }

    public void ActionOrNot(int queryChoice, String[] params) {
    	  if(this.isActionQuery(queryChoice)) {
   		   this.ExecuteUpdate(queryChoice, params);
              int numOfRowAffected = this.GetUpdateAmount();
              System.out.println("Number of Rows: " + numOfRowAffected);
   	   }else {
   		   this.ExecuteQuery(queryChoice, params);
              String[][] results = this.GetQueryData();
              for (int row = 0; row < results.length; row++) {
            	  for (int col = 0; col < results[row].length; col++) {
            		  System.out.print(results[row][col] + "\t");
            	  }
            	  System.out.println();
              }
   	   }    }


    public boolean Connect(String szHost, String szUser, String szPass, String szDatabase)
    {

        boolean bConnect = m_jdbcData.ConnectToDatabase(szHost, szUser, szPass, szDatabase);
        if (bConnect == false)
            m_error = m_jdbcData.GetError();
        return bConnect;
    }

    public boolean Disconnect()
    {
        // Disconnect the JDBCData Object
        boolean bConnect = m_jdbcData.CloseDatabase();
        if (bConnect == false)
            m_error = m_jdbcData.GetError();
        return true;
    }

    public String GetError()
    {
        return m_error;
    }

    private QueryJDBC m_jdbcData;
    private String m_error;
    private String m_projectTeamApplication;
    private ArrayList<QueryData> m_queryArray;
    private int m_updateAmount;

    /**
     * @param args the command line arguments
     */

    public static void showWelcome(int n) {
    	System.out.println("\n===============================================");
    	System.out.println("Welcome to the Parking Warning Database System.");
    	System.out.println("===============================================");
        System.out.printf("This program will execute the following %d queries.\n", n);

        String[] queryDescriptions = new String[]{
            "Show all records.",
            "Show records of specific owner ID.",
            "Show records of a city.",
            "Insert a new record.",
            "Show top 5 cities with most citations.",
            "Show sum of citations for age tiers.",
            "Compare cities of citations to cities of driver licenses.",
            "Show adoption of this app, by mobile carriers.",
            "Show sums of mobile carrier and phone brand.",
            "Show sum of citations for each time of day.",
            "Show monthly sum of citations.",
            "Show sum of citations for each mobile carrier."
        };

        for (int i = 0; i < n; i++) {
            System.out.println(i+1 + ")" + queryDescriptions[i]);
        }
    }

    public static void main(String[] args) {
        final QueryRunner queryrunner = new QueryRunner();

        if (args.length == 0)
        {
            java.awt.EventQueue.invokeLater(new Runnable() {
                public void run() {

                    new QueryFrame(queryrunner).setVisible(true);
                }
            });
        }
        else
        {
            if (args[0].equals ("-console"))
            {
                QueryRunner qr = new QueryRunner();
                int n = qr.GetTotalQueries();
                Scanner keyboard = new Scanner(System.in);
            	String str;
                // preset login credentials
                String PasswordField1 = "mm_sttest1bPass";
                String TextHostname = "cs100.seattleu.edu";
                String TextFieldUser = "mm_sttest1b";
                String TextFieldDatabase = "mm_sttest1b_3nf";
                qr.Connect(TextHostname, TextFieldUser, PasswordField1, TextFieldDatabase);

                showWelcome(n);

                for (int i = 0; i < n; i++) {
                    System.out.print("\nPlease press 'Enter' to continue, or 'q' to quit.  ");
                    str = keyboard.nextLine();
                    if (str.length() != 0 && str.charAt(0) == 'q') {
                        break;
                    }
                    System.out.println(qr.GetQueryText(i));
                    if (qr.isParameterQuery(i)) {
                	   int amt = qr.GetParameterAmtForQuery(i);
                	   String[] params = new String[amt];
                	   for (int j=0; j< amt; j++) {
                		   	System.out.println(qr.GetParamText(i, j));
                	   		String line = keyboard.nextLine();
                	   		params[j] = line.trim();
                	   }
                	   qr.ActionOrNot(i,params);
                   } else {
                	   String[] params = {};
                	   qr.ActionOrNot(i,params);
                   }
                }
                System.out.print("\n\nThank you for using the Parking Warning Database System. Goodbye.");
                keyboard.close();
                qr.Disconnect();
            }
        }
    }
}
