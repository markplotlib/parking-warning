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


        // TODO - You will need to change the queries below to match your queries.

        // You will need to put your Project Application in the below variable

        this.m_projectTeamApplication="PARKING WARNING";

        // Each row that is added to m_queryArray is a separate query. It does not work on Stored procedure calls.
        // The 'new' Java keyword is a way of initializing the data that will be added to QueryArray. Please do not change
        // Format for each row of m_queryArray is: (QueryText, ParamaterLabelArray[], LikeParameterArray[], IsItActionQuery, IsItParameterQuery)

        //    QueryText is a String that represents your query. It can be anything but Stored Procedure
        //    Parameter Label Array  (e.g. Put in null if there is no Parameters in your query, otherwise put in the Parameter Names)
        //    LikeParameter Array  is an array I regret having to add, but it is necessary to tell QueryRunner which parameter has a LIKE Clause. If you have no parameters, put in null. Otherwise put in false for parameters that don't use 'like' and true for ones that do.
        //    IsItActionQuery (e.g. Mark it true if it is, otherwise false)
        //    IsItParameterQuery (e.g.Mark it true if it is, otherwise false)

        m_queryArray.add(new QueryData("Select * from owner", null, null, false, false));
        m_queryArray.add(new QueryData("Select * from owner where owner_id=?", new String [] {"OWNER_ID"}, new boolean [] {false},  false, true));
        m_queryArray.add(new QueryData("Select * from location where city like ?", new String [] {"CITY"}, new boolean [] {true}, false, true));
        m_queryArray.add(new QueryData("insert into owner (owner_id, first_name, last_name, age, phone_number) values (?,?,?,?,?)",new String [] {"OWNER_ID", "FIRST_NAME", "LAST_NAME", "AGE", "PHONE_NUMBER"}, new boolean [] {false, false, false, false, false}, true, true));

        // Query numbers below correspond to the query verifying sheet in google drive
        // Query #2
        m_queryArray.add(new QueryData("SELECT inc.location_id, loc.city, loc.state, COUNT(inc.location_id) AS Incidents\r\n" +
        		"FROM incident as inc, location as loc WHERE inc.location_id = loc.location_id\r\n" +
        		"GROUP BY inc.location_id, loc.city, loc.state ORDER BY Incidents DESC Limit 5;", null, null, false, false));   // THIS NEEDS TO CHANGE FOR YOUR APPLICATION

        // Query #3
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

        // Query #4
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

     // Query #5
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

     // Query #6
        m_queryArray.add(new QueryData(
        		"SELECT p.phone_brand, m.mobile_carrier_name,\n" +
        		"COUNT(m.mobile_carrier_name) -- AS [total mobile carrier users]\n" +
        		"FROM phone p\n" +
        		"JOIN mobile_carrier m ON p.mobile_carrier =\n" +
        		"m.mobile_carrier_id\n" +
        		"GROUP BY m.mobile_carrier_name, p.phone_brand\n" +
        		"ORDER BY p.phone_brand, m.mobile_carrier_name\n"
        		, null, null, false, false));

     // Query #7
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

     // Query #9
        m_queryArray.add(new QueryData(
        		"SELECT\n" +
        		"SUM(CASE WHEN MONTH(datetime) = 11\n" +
        		"AND YEAR(datetime) = 2016 THEN 1 ELSE 0 END)\n" +
        		"AS 'Total Number of Warnings given in November 2016'\n" +
        		"FROM incident\n" +
        		"WHERE outcome_id = 0;\n"
        		, null, null, false, false));

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



    public static void main(String[] args) {
        // TODO code application logic here

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
                Scanner scan = new Scanner(System.in);
                System.out.println("Welcome to the Parking Warning System.");
                System.out.println("Menu:");
                System.out.println("1) option 1");
                System.out.println("2) option 2");
                System.out.println("3) option 3");
                System.out.print("Please enter a menu option: ");
                String userInput = scan.nextLine();
                System.out.println(userInput);

                //    You need to determine if it is a parameter query. If it is, then
                //    you will need to ask the user to put in the values for the Parameters in your query
                //    you will then call ExecuteQuery or ExecuteUpdate (depending on whether it is an action query or regular query)
                //    if it is a regular query, you should then get the data by calling GetQueryData. You should then display this
                //    output.
                //    If it is an action query, you will tell how many row's were affected by it.
                //
                //    This is Psuedo Code for the task:
                //    Connect()
                //    n = GetTotalQueries()
                //    for (i=0;i < n; i++)
                //    {
                //       Is it a query that Has Parameters
                //       Then
                //           amt = find out how many parameters it has
                //           Create a paramter array of strings for that amount
                //           for (j=0; j< amt; j++)
                //              Get The Paramater Label for Query and print it to console. Ask the user to enter a value
                //              Take the value you got and put it into your parameter array
                //           If it is an Action Query then
                //              call ExecuteUpdate to run the Query
                //              call GetUpdateAmount to find out how many rows were affected, and print that value
                //           else
                //               call ExecuteQuery
                //               call GetQueryData to get the results back
                //               print out all the results
                //           end if
                //      }
                //    Disconnect()


                // NOTE - IF THERE ARE ANY ERRORS, please print the Error output
                // NOTE - The QueryRunner functions call the various JDBC Functions that are in QueryJDBC. If you would rather code JDBC
                // functions directly, you can choose to do that. It will be harder, but that is your option.
                // NOTE - You can look at the QueryRunner API calls that are in QueryFrame.java for assistance. You should not have to
                //    alter any code in QueryJDBC, QueryData, or QueryFrame to make this work.
//                System.out.println("Please write the non-gui functionality");

            }
        }

    }
}
