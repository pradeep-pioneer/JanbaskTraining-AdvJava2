package com.janbask.training3;

import javax.sql.RowSet;
import javax.sql.rowset.JdbcRowSet;
import javax.sql.rowset.RowSetProvider;
import java.sql.SQLException;

public class JdbcRowSetEx {
    public static void main(String[] args) {
        rowSetDefaultExample();
    }

    static void rowSetDefaultExample(){
        try {
            //1. Get a RowSet instance from Factory (study the Factory design pattern on Wikipedia)
            JdbcRowSet rowSet = RowSetProvider.newFactory().createJdbcRowSet();
            //2. Configure connection properties on RowSet
            rowSet.setUrl(Config.DATABASE_URL);
            rowSet.setUsername(Config.DATABASE_USERNAME);
            rowSet.setPassword(Config.DATABASE_PASSWORD);
            //3. Set Command on RowSet instance
            rowSet.setCommand("select * from ut_category");
            //4. Execute the RowSet Command
            rowSet.execute();
            //5 . Read the RowSet items
            while (rowSet.next()){
                String categoryName = rowSet.getString(2);
                String categoryDescription = rowSet.getString(3);
                System.out.printf("\nCategory Name: %s\t\t\tDescription: %s", categoryName, categoryDescription);
            }
            //6. Cleanup the connection
            rowSet.close();
        }catch (SQLException exception){
            exception.printStackTrace();
        }
    }
}
