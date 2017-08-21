package com.janbask.training3;

import java.sql.*;
import java.util.Random;
import java.util.stream.IntStream;

public class TransactionsEx {
    private static final String BATCH_INSERT_PROCEDURE_CALL = "call usp_insert_bulkbatch(?, ?, ?);";
    public static void main(String[] args) {
        Connection connection = null;
        try {
            //1. Get connection
            connection = DriverManager.getConnection(Config.DATABASE_URL, Config.DATABASE_USERNAME, Config.DATABASE_PASSWORD);
            //2. Set autocommit to false so that we can commit all the batch updates at once (to avoid partial inserts if there is some error)
            connection.setAutoCommit(false);
            //3. get a callable statement - this could be a normal text statement as well (i.e. prepared statement)
            CallableStatement statement = connection.prepareCall(BATCH_INSERT_PROCEDURE_CALL);
            //4. this would be used to fill some random values
            Random random = new Random();
            //5. run a simple loop to add batched values to the statement
            for (int i = 1; i <= 999; i++){
                String batchName = "Batch "+ i;
                String batchValue = "Batch Value " + (i * random.nextInt(9999));
                String description = String.format("This is %s numbered description, we are trying to get batch data at %s.", random.nextDouble(), random.nextInt());
                statement.setString("batchName",batchName);
                statement.setString("batchValue", batchValue);
                statement.setString("description", description);
                statement.addBatch();
            }
            //6. finally execute the batch statements
            int[] count = statement.executeBatch();
            //7. Create a new Statement
            Statement statementBad = connection.createStatement();
            //Submit a malformed SQL statement that breaks
            String SQL = "INSERTED IN ut_Test  " +
                    "VALUES (107, 22, 'Bad', 'Statement')";
            statementBad.executeUpdate(SQL);

            //8. commit all the batch operations (important - if you do not commit the changes would not be persisted - experiment by commenting the below line)
            connection.commit();

            //9. perform cleanup
            statement.close();
            connection.close();

            //10. Now show how many records are inserted - Notice the use of IntStream (it converts a collection into a stream and then we can perform map reduce operations on it)
            int totalRecordsUpdated = IntStream.of(count).sum();
            System.out.println(String.format("Total Records Inserted: %s", totalRecordsUpdated));
        }catch (SQLException exception){
            System.out.printf("\nThere was an error in the transaction.\nError: %s\nStackTrace:%s\nTrying to rollback the transaction and close connection...",
                    exception.getMessage(), exception.getStackTrace());
            if(connection!=null) {
                try {

                    connection.rollback();
                    connection.close();
                }catch (SQLException exceptionInner){
                    exceptionInner.printStackTrace();
                }
            }
        }
    }
}
