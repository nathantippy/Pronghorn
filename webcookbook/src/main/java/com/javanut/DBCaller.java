package com.javanut;

import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javanut.json.encode.JSONRenderer;
import com.javanut.pronghorn.network.HTTPUtilResponse;
import com.javanut.pronghorn.network.config.HTTPContentTypeDefaults;
import com.javanut.pronghorn.network.http.HTTPUtil;
import com.javanut.pronghorn.network.http.HeaderWritable;
import com.javanut.pronghorn.network.schema.HTTPRequestSchema;
import com.javanut.pronghorn.network.schema.ServerResponseSchema;
import com.javanut.pronghorn.pipe.ChannelWriter;
import com.javanut.pronghorn.pipe.DataInputBlobReader;
import com.javanut.pronghorn.pipe.Pipe;
import com.javanut.pronghorn.pipe.StructuredReader;
import com.javanut.pronghorn.pipe.util.MutableCharReader;
import com.javanut.pronghorn.stage.blocking.Blockable;
import com.javanut.pronghorn.util.AppendableByteWriter;
import com.javanut.pronghorn.util.StringBuilderWriter;

public class DBCaller extends Blockable<HTTPRequestSchema,ServerResponseSchema,ServerResponseSchema> {

	private static final String DB_DRIVER = "org.h2.Driver";
	private static final String DB_CONNECTION = "jdbc:h2:~/webCookbookDB";
	private static final String DB_USER = "";
	private static final String DB_PASSWORD = "";
	
	private long channelId;
	private int sequence;
	private int verb;
	private int revision;
	private int context;

	private int requestId;
	private String name;
	
	StringBuilderWriter payloadBuffer = new StringBuilderWriter();
	
	public final HTTPUtilResponse ebh = new HTTPUtilResponse();
	
    private static final Logger logger = LoggerFactory.getLogger(DBCaller.class);
    
	public DBCaller() {
		
        try {
            Class.forName(DB_DRIVER);
            try {
            	// delete the H2 database
            	DeleteDbFiles.execute("~", "webCookbookDB", true);
            	createTables();
            } catch (SQLException e) {
            	e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
        	logger.info("unable to load driver",e);
        }
  
	}
								
	@Override
	public boolean begin(Pipe<HTTPRequestSchema> input) {

		int msgIdx = Pipe.takeMsgIdx(input);
		channelId = Pipe.takeLong(input);
		sequence = Pipe.takeInt(input);
		verb = Pipe.takeInt(input);
									
		DataInputBlobReader<HTTPRequestSchema> params = Pipe.openInputStream(input);
		StructuredReader reader = params.structured();
		
		requestId = reader.readInt(WebFields.id);
		if (requestId>=0) {
			name = reader.readText(WebFields.name);
		}

		revision = Pipe.takeInt(input);
		context = Pipe.takeInt(input);
		
		Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
		Pipe.releaseReadLock(input);
				
		return true;
	}

	@Override
	public void run() throws InterruptedException {

        try {
        	
        	//write if that was requested
        	if (requestId>=0) {
        		insertData(requestId, name);
        	}
        	
        	//read all the data from the database
        	payloadBuffer.clear();
        	readData(payloadBuffer); 	
        	
        } catch (SQLException e) {
            e.printStackTrace();
        }		
	}

	@Override
	public void finish(Pipe<ServerResponseSchema> output) {

		ChannelWriter outputStream = HTTPUtilResponse.openHTTPPayload(ebh, output, 
				                     channelId, 
				                     sequence);
				
		outputStream.append(payloadBuffer);
	
		HeaderWritable additionalHeaderWriter = null;
		
		HTTPUtilResponse.closePayloadAndPublish(
				ebh, null, HTTPContentTypeDefaults.JSON, 
				output, channelId, sequence, context, 
				outputStream, additionalHeaderWriter, 200);
				
		
	}

	@Override
	public void timeout(Pipe<ServerResponseSchema> output) {
		
		HTTPUtil.publishStatus(channelId, sequence, 404, output);
		
	}


    // H2 SQL Statement Example
    private static void createTables() throws SQLException {
        Connection connection = getDBConnection();
        Statement stmt = null;
        try {
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
            stmt.execute("CREATE TABLE PERSON(id int primary key, name varchar(255))");
            stmt.close();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }
    
    private static void insertData(int id, String name) throws SQLException {
        Connection connection = getDBConnection();

        try {
            connection.setAutoCommit(false);

            PreparedStatement insertPreparedStatement = connection.prepareStatement("INSERT INTO PERSON(id, name) values(?,?)");
            insertPreparedStatement.setInt(1, id);
			insertPreparedStatement.setString(2, name);
            insertPreparedStatement.executeUpdate();
            insertPreparedStatement.close();
            
            connection.commit();
        } catch (SQLException e) {
        	logger.info("unable to insert",e);
        } catch (Exception e) {
        	logger.info("unable to insert",e);
        } finally {
            connection.close();
        }
    }
    
   
	private static final JSONRenderer<ResultSet> jsonRenderer = new JSONRenderer<ResultSet>()
			  .startObject()
			  .integer("id", r->{
						try {
							return r.getInt("id");
						} catch (SQLException e) {
							e.printStackTrace();
							return -1;
						}
			})
			  .string("name", (r,t) -> {
				  try {
						t.append(r.getString("name"));
					} catch (SQLException e) {
						e.printStackTrace();
						t.append(null);
					}})
			  .endObject();
    
    
    private static void readData(AppendableByteWriter<?> target) throws SQLException {
        Connection connection = getDBConnection();
        Statement stmt = null;
        try {
            stmt = connection.createStatement();

            ResultSet rs = stmt.executeQuery("select * from PERSON");
            
            while (rs.next()) {            	
            	jsonRenderer.render(target, rs);
            	target.append('\n');
            }
            
            stmt.close();
            connection.commit();
        } catch (SQLException e) {
        	logger.info("unable to read",e);
        } catch (Exception e) {
        	logger.info("unable to read",e);
        } finally {
            connection.close();
        }
    }

    private static Connection getDBConnection() {
        
        try {
        	return DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
        } catch (SQLException e) {
        	logger.info("unable to open connection",e);
        }
        return null;
    }
}

