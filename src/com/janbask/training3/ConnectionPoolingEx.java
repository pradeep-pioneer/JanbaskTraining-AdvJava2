package com.janbask.training3;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.MysqlDataSourceFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class ConnectionPoolingEx {
    public static void main(String[] args) throws Exception {
        //1. Get the Connection instance from DataSource
        Connection connection = MyDataSourceFactory.getMySQLConnection();
        //3. Creating a statement
        String sql = "select * from ut_category";
        PreparedStatement statement = connection.prepareStatement(sql);
        //4. Executing a statement
        ResultSet resultSet = statement.executeQuery();
        //5. Retrieving values from ResultSet
        while(resultSet.next()){
            String categoryName = resultSet.getString(2);
            String categoryDescription = resultSet.getString(3);
            System.out.printf("\nCategory Name: %s\t\t\tDescription: %s", categoryName, categoryDescription);
        }
        //6. Cleanup
        resultSet.close();
        statement.close();
        connection.close();
    }
}
class MyDataSourceFactory {

    public static Connection getMySQLConnection() throws Exception {
        Properties props = new Properties();
        InputStream is = new FileInputStream("db.properties");
        props.load(is);
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUrl(props.getProperty("MYSQL_DB_URL"));
        ds.setUser(props.getProperty("MYSQL_DB_USERNAME"));
        ds.setPassword(props.getProperty("MYSQL_DB_PASSWORD"));
        return ds.getConnection();
    }
}