package com.janbask.training3;

import java.sql.*;
import java.util.Random;
import java.util.stream.IntStream;

public class Main {

    private static final String BATCH_INSERT_PROCEDURE_CALL = "call usp_insert_bulkbatch(?, ?, ?);";
    public static void main(String[] args) {
	    //1. Check for batch processing capabilities
        checkDatabaseMetadata();

        //2. Perform batch inserts
        performBatchOperation();
    }

    static void checkDatabaseMetadata(){
        try {
            //1. Creating a connection
            Connection connection = DriverManager.getConnection(Config.DATABASE_URL, Config.DATABASE_USERNAME, Config.DATABASE_PASSWORD);
            //2. Fetch Metadata Object
            DatabaseMetaData md = connection.getMetaData();
            //3. Start listing the database server and database properties.
            System.out.println("getURL() - " + md.getURL());
            System.out.println("getUserName() - " + md.getUserName());
            System.out.println("getDatabaseProductVersion - " + md.getDatabaseProductVersion());
            System.out.println("getDriverMajorVersion - " + md.getDriverMajorVersion());
            System.out.println("getDriverMinorVersion - " + md.getDriverMinorVersion());
            System.out.println("nullAreSortedHigh - " + md.nullsAreSortedHigh());

            System.out.println("Feature Support");
            System.out.println("supportsAlterTableWithDropColumn - "
                    + md.supportsAlterTableWithDropColumn());
            System.out.println("supportsBatchUpdates - " + md.supportsBatchUpdates());
            System.out.println("supportsTableCorrelationNames - " + md.supportsTableCorrelationNames());
            System.out.println("supportsPositionedDelete - " + md.supportsPositionedDelete());
            System.out.println("supportsFullOuterJoins - " + md.supportsFullOuterJoins());
            System.out.println("supportsStoredProcedures - " + md.supportsStoredProcedures());
            System.out.println("supportsMixedCaseQuotedIdentifiers - "
                    + md.supportsMixedCaseQuotedIdentifiers());
            System.out.println("supportsANSI92EntryLevelSQL - " + md.supportsANSI92EntryLevelSQL());
            System.out.println("supportsCoreSQLGrammar - " + md.supportsCoreSQLGrammar());
            System.out.println("getMaxRowSize - " + md.getMaxRowSize());
            System.out.println("getMaxStatementLength - " + md.getMaxStatementLength());
            System.out.println("getMaxTablesInSelect - " + md.getMaxTablesInSelect());
            System.out.println("getMaxConnections - " + md.getMaxConnections());
            System.out.println("getMaxCharLiteralLength - " + md.getMaxCharLiteralLength());

            System.out.println("getTableTypes()");
            ResultSet rs = md.getTableTypes();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

            System.out.println("getTables()");
            rs = md.getTables("janbask_training", "", "%", new String[0]);
            while (rs.next()) {
                System.out.println(rs.getString("TABLE_NAME"));
            }
            System.out.println("Transaction Support");
            System.out.println("getDefaultTransactionIsolation() - "
                    + md.getDefaultTransactionIsolation());
            System.out.println("dataDefinitionIgnoredInTransactions() - "
                    + md.dataDefinitionIgnoredInTransactions());

            System.out.println("General Source Information");
            System.out.println("getMaxTablesInSelect - " + md.getMaxTablesInSelect());
            System.out.println("getMaxColumnsInTable - " + md.getMaxColumnsInTable());
            System.out.println("getTimeDateFunctions - " + md.getTimeDateFunctions());
            System.out.println("supportsCoreSQLGrammar - " + md.supportsCoreSQLGrammar());

            System.out.println("getTypeInfo()");
            rs = md.getTypeInfo();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

            //4. Cleanup
            rs.close();
            connection.close();
        }
        catch (SQLException exception){
            exception.printStackTrace();
        }
    }

    static void performBatchOperation(){
        try {
            //1. Get connection
            Connection connection = DriverManager.getConnection(Config.DATABASE_URL, Config.DATABASE_USERNAME, Config.DATABASE_PASSWORD);
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
            //7. commit all the batch operations (important - if you do not commit the changes would not be persisted - experiment by commenting the below line)
            connection.commit();

            //8. perform cleanup
            statement.close();
            connection.close();

            //9. Now show how many records are inserted - Notice the use of IntStream (it converts a collection into a stream and then we can perform map reduce operations on it)
            int totalRecordsUpdated = IntStream.of(count).sum();
            System.out.println(String.format("Total Records Inserted: %s", totalRecordsUpdated));
        }catch (SQLException exception){
            exception.printStackTrace();
        }
    }
}
