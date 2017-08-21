package com.janbask.training3;

import com.sun.rowset.CachedRowSetImpl;

import java.sql.*;

public class CachedRowSetEx {

    public static void main(String[] args) {
        //1. Get CachedRowSet (Notice that we have closed the connection before getting the CachedRowSet so data is cached in the memory).
        try {
            CachedRowSetImpl cachedRowSet = getCachedRowSet();
            while (cachedRowSet.next()){
                String categoryName = cachedRowSet.getString(2);
                String categoryDescription = cachedRowSet.getString(3);
                System.out.printf("\nCategory Name: %s\t\t\tDescription: %s", categoryName, categoryDescription);
            }
        }catch (SQLException exception){
            exception.printStackTrace();
        }
    }

    static CachedRowSetImpl getCachedRowSet() throws SQLException{
        //1. Creating a connection
        Connection connection = DriverManager.getConnection(Config.DATABASE_URL, Config.DATABASE_USERNAME, Config.DATABASE_PASSWORD);
        //2. Creating a statement
        String sql = "select * from ut_category";
        PreparedStatement statement = connection.prepareStatement(sql);
        //3. Executing a statement
        ResultSet resultSet = statement.executeQuery();
        //4. Populate a CachedRowSet from ResultSet
        CachedRowSetImpl cachedRowSet = new CachedRowSetImpl();
        cachedRowSet.populate(resultSet);
        //5. Cleanup
        resultSet.close();
        statement.close();
        connection.close();
        return cachedRowSet;
    }
}
