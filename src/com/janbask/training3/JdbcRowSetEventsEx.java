package com.janbask.training3;

import javax.sql.RowSetEvent;
import javax.sql.RowSetListener;
import javax.sql.rowset.JdbcRowSet;
import javax.sql.rowset.RowSetProvider;

public class JdbcRowSetEventsEx {
    public static void main(String[] args) throws Exception {

        //1. Creating and Executing RowSet
        JdbcRowSet rowSet = RowSetProvider.newFactory().createJdbcRowSet();
        //2. Configure connection properties on RowSet
        rowSet.setUrl(Config.DATABASE_URL);
        rowSet.setUsername(Config.DATABASE_USERNAME);
        rowSet.setPassword(Config.DATABASE_PASSWORD);

        //3. Set Command on RowSet instance
        rowSet.setCommand("select * from ut_category");

        //4. Execute the RowSet Command
        rowSet.execute();

        //5. Adding Listener and moving RowSet
        rowSet.addRowSetListener(new MyListener());

        //6 . Read the RowSet items
        while (rowSet.next()) {
            //7. Each next() call generates cursor Moved event
            String categoryName = rowSet.getString(2);
            String categoryDescription = rowSet.getString(3);

            System.out.printf("\nCategory Name: %s\t\t\tDescription: %s", categoryName, categoryDescription);
        }
        //8. Cleanup the connection
        rowSet.close();
    }
}

class MyListener implements RowSetListener {
    public void cursorMoved(RowSetEvent event) {
        System.out.println("\nCursor Moved...");
    }
    public void rowChanged(RowSetEvent event) {
        System.out.println("\nCursor Changed...");
    }
    public void rowSetChanged(RowSetEvent event) {
        System.out.println("\nRowSet changed...");
    }
}
