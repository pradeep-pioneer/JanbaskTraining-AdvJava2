package com.janbask.training3;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.time.LocalDateTime;

public class BlobEx {
    static final String FILE_URL = "http://www.mysqltutorial.org/download/18";
    static final String INSERT_LOB_DATA_CALL = "call usp_insert_largeobject(?, ?)";
    static final String BLOB_DATA_INPUT_PARAM ="blobData";
    static final String TEXT_DATA_INPUT_PARAM = "clobData";
    public static void main(String[] args) {
        insertBlobData();
    }

    static void insertBlobData(){
        try {
            //1. Get connection
            System.out.printf("Preparing Connection: %s\n", LocalDateTime.now());
            Connection connection = DriverManager.getConnection(Config.DATABASE_URL, Config.DATABASE_USERNAME, Config.DATABASE_PASSWORD);
            System.out.printf("Preparing Statement: %s\n", LocalDateTime.now());
            CallableStatement statement = connection.prepareCall(INSERT_LOB_DATA_CALL);
            System.out.printf("Preparing File Download Stream: %s\n", LocalDateTime.now());
            InputStream streamBlob = getHttpStreamForBlobData();
            if(streamBlob!=null) {
                System.out.printf("Setting statement parameter <blobData=InputStream>: %s\n", LocalDateTime.now());
                statement.setBinaryStream(BLOB_DATA_INPUT_PARAM, getHttpStreamForBlobData());
                System.out.printf("Setting statement parameter <clobData=null>: %s\n", LocalDateTime.now());
                statement.setString(TEXT_DATA_INPUT_PARAM, null);
                System.out.printf("Inserting data into table (calling execute on statement): %s\n", LocalDateTime.now());
                int affectedRecords = statement.executeUpdate();
                System.out.printf("Finished: %s\n", LocalDateTime.now());
                System.out.printf("\n%s records inserted!", affectedRecords);
                System.out.printf("\n\nPerforming Cleanup: %s\n", LocalDateTime.now());
                statement.close();
                streamBlob.close();
                connection.close();
            }else
                System.out.printf("\nFailed to get input stream");
        }catch (SQLException exception){
            exception.printStackTrace();
        }catch (IOException exception){
            exception.printStackTrace();
        }
    }

    static InputStream getHttpStreamForBlobData(){
        InputStream inputStream = null;
        try {
            URL url = new URL(FILE_URL);
            HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
            int responseCode = httpConn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK){
                inputStream = httpConn.getInputStream();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }catch (IOException exception){
            exception.printStackTrace();
        }
        return inputStream;
    }
}
