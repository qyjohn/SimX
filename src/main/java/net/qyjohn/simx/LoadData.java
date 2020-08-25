package net.qyjohn.simx;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.io.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;

public class LoadData extends Thread
{
	int id;
	String bucket;
	ConcurrentLinkedQueue<String> queue;
	
	// The Table
	// CREATE TABLE raw (uid VARCHAR(25), ds VARCHAR(25), ts INT, latitude DOUBLE, longitude DOUBLE);
	// After data import is completed, create the following three index
	// CREATE INDEX ts ON raw (ts);
	// CREATE INDEX latitude ON raw (latitude);
	// CREATE INDEX longitude ON raw (longitude);
	
	public LoadData(int id, String bucket, ConcurrentLinkedQueue<String> queue)
	{
		this.id     = id;
		this.bucket = bucket;	
		this.queue  = queue;
	}

	public void run()
	{
		try
		{
			// Create an S3 Client
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

			// Getting database properties from db.properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("db.properties");
			prop.load(input);
			String db_hostname = prop.getProperty("db_hostname");
			String db_username = prop.getProperty("db_username");
			String db_password = prop.getProperty("db_password");
			String db_database = prop.getProperty("db_database");
			String jdbc_url = "jdbc:mysql://" + db_hostname + "/" + db_database + "?user=" + db_username + "&password=" + db_password;

			// Create a JDBC connection to the database
			Connection conn = DriverManager.getConnection(jdbc_url);
			
			// Date data format
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
			
			// Poll from the queue for work to do
			String key = queue.poll();
			while (key != null)
			{
				// Read the S3 object
				S3Object object   = s3Client.getObject(bucket, key);
				System.out.println("Thread " + id + ": " + bucket + "\t" + key);
				BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
				
				String line;
				int records = 0;
				while ((line = reader.readLine()) != null) 
				{
					try
					{
						// process the line.
						JSONParser parser = new JSONParser();
						JSONObject jsonObject = (JSONObject) parser.parse(line);
						String ds = (String) jsonObject.get("date");
						String dz = ds.substring(0, 23) + "+0000";
						Date dt = sdf.parse(dz);
						long epoch = dt.getTime();
						int ts = (int) (epoch / 1000);
						String uid = (String) jsonObject.get("uid");
						double latitude = (double) jsonObject.get("latitude");
						double longitude = (double) jsonObject.get("longitude");
						
						// PreparedStatement for Inserts
						PreparedStatement statement = conn.prepareStatement("INSERT INTO raw (uid, ds, ts, latitude, longitude) VALUES (?, ?, ?, ?, ?)");
						statement.setString(1, uid);
						statement.setString(2, ds);
						statement.setInt(3, ts);
						statement.setDouble(4, latitude);
						statement.setDouble(5, longitude);
						statement.executeUpdate();
					} catch (Exception e1)
					{
						// Simply ignore the exception here. 
						// We don't need to deal with bad data at all.
						System.out.println(e1.getMessage());
						e1.printStackTrace();
					}
				}
				reader.close();	
				// Poll the queue again for work to do
				key = queue.poll();
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	
	
	public static void main(String[] args)
	{
		try
		{
			// Create a queue to distribute the work
			ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
			// Load all S3 objects under s3://args[0]/args[1]/ into the queue
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
			ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(args[0]).withPrefix(args[1]).withMaxKeys(2);
			ListObjectsV2Result result;
			do 
			{
				result = s3Client.listObjectsV2(req);
				for (S3ObjectSummary objectSummary : result.getObjectSummaries()) 
				{
					queue.add(objectSummary.getKey());
				}
				String token = result.getNextContinuationToken();
				req.setContinuationToken(token);
			} while (result.isTruncated());
			// Start multiple threads to ingest data into database
			int nProc = Runtime.getRuntime().availableProcessors();
			LoadData workers[] = new LoadData[nProc];
			for (int i=0; i<nProc; i++)
			{
				workers[i] = new LoadData(i, args[0], queue);
				workers[i].start();
			}
			// Wait for the workers to complete
			for (int i=0; i<nProc; i++)
			{
				workers[i].join();
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
