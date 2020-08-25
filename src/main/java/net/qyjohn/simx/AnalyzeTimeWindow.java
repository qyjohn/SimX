package net.qyjohn.simx;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.LinkedList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class AnalyzeTimeWindow extends Thread
{
	int id, ts1, ts2;
	double MIN_LATITUDE = 50.00;
	double MAX_LATITUDE = 60.00;
	double STEP_WIDTH   = 0.1;
	double STEP_MARGIN  = 0.05;
	int TOTAL_STEPS     = (int) Math.ceil(MAX_LATITUDE / STEP_WIDTH - MIN_LATITUDE / STEP_WIDTH);
	double TEST_LONGITUDE_RANGE = 0.05;
	double SOCIAL_DISTANCE = 100.00;	// 100 meters
	double TIME_DISTANCE = 600; // 600 seconds

	// CREATE TABLE results (uid_1 VARCHAR(25), ds_1 VARCHAR(25), ts_1 INT, latitude_1 DOUBLE, longitude_1 DOUBLE, 
    // uid_2 VARCHAR(25), ds_2 VARCHAR(25), ts_2 INT, latitude_2 DOUBLE, longitude_2 DOUBLE,
    // time_distance INT, social_distance DOUBLE, md5 VARCHAR(32));
    
	public AnalyzeTimeWindow(int id, int ts1, int ts2, int time, double distance)
	{
		this.id  = id;
		this.ts1 = ts1;	
		this.ts2 = ts2;	
		this.TIME_DISTANCE   = time;
		this.SOCIAL_DISTANCE = distance;
	}


	public double calculateDistance(DataPoint x, DataPoint y)
	{
		double RADIUS = 6378000;
		double dlat = Math.toRadians(y.latitude  - x.latitude);
		double dlon = Math.toRadians(y.longitude - x.longitude);
		double a = (Math.sin(dlat / 2) * Math.sin(dlat / 2)) + Math.cos(Math.toRadians(x.latitude)) * Math.cos(Math.toRadians(y.latitude)) * (Math.sin(dlon / 2) * Math.sin(dlon / 2));
		double angle = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		return angle * RADIUS;
	}
	
	
	public void run()
	{
		try
		{
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
			
			// Vertically scaning through the map
			LinkedList<DataPair> pairs = new LinkedList<DataPair>();
			for (int i=0; i<TOTAL_STEPS; i++)
			{
				double latitude1 = MIN_LATITUDE + i * STEP_WIDTH - STEP_MARGIN;
				double latitude2 = MIN_LATITUDE + (i+1) * STEP_WIDTH + STEP_MARGIN;
				PreparedStatement statement =conn.prepareStatement("SELECT * from raw WHERE ts >= ? AND ts <= ? AND latitude >= ? AND latitude <= ? ORDER BY longitude ASC");
				statement.setInt(1, ts1);
				statement.setInt(2, ts2);
				statement.setDouble(3, latitude1);
				statement.setDouble(4, latitude2);
				ResultSet rs = statement.executeQuery();
				
				LinkedList<DataPoint> list = new LinkedList<DataPoint>();
				while(rs.next()) 
				{
					DataPoint data = new DataPoint();
					data.uid = rs.getString("uid");
					data.ds  = rs.getString("ds");
					data.ts  = rs.getInt("ts");
					data.latitude  = rs.getDouble("latitude");
					data.longitude = rs.getDouble("longitude");
					list.add(data);
				}

				int size = list.size();
				boolean test = true;
				// Horizontally scaning through the stripe
				for (int j=0; j<size; j++)
				{
					DataPoint p1 = list.get(j);
					for (int k=j+1; k<size && test; k++)
					{
						DataPoint p2 = list.get(k);
						double delta = p2.longitude - p1.longitude;
						if (delta < TEST_LONGITUDE_RANGE)
						{
							// Calculate the time difference and distance between the two points
							int time_distance = Math.abs(p2.ts - p1.ts);
							if (time_distance < TIME_DISTANCE)
							{
								double social_distance = calculateDistance(p1, p2);
								if (social_distance <= SOCIAL_DISTANCE)
								{
									DataPair pair = new DataPair(p1, p2, time_distance, social_distance);
									pairs.add(pair);
								}
							}
						}
						else
						{
							// Do not need to test any data points on the far right
							test = false;
						}
					}
				}
			}
			
			// Scaning completed
			// Write the results to database
			System.out.println("Total data pairs: " + pairs.size());
			for (DataPair pair: pairs)
			{
				PreparedStatement statement = conn.prepareStatement("INSERT INTO results (uid_1, ds_1, ts_1, latitude_1, longitude_1, uid_2, ds_2, ts_2, latitude_2, longitude_2, time_distance, social_distance, md5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				statement.setString(1, pair.p1.uid);
				statement.setString(2, pair.p1.ds);
				statement.setInt(3, pair.p1.ts);
				statement.setDouble(4, pair.p1.latitude);
				statement.setDouble(5, pair.p1.longitude);
				statement.setString(6, pair.p2.uid);
				statement.setString(7, pair.p2.ds);
				statement.setInt(8, pair.p2.ts);
				statement.setDouble(9, pair.p2.latitude);
				statement.setDouble(10, pair.p2.longitude);
				statement.setInt(11, pair.time_distance);
				statement.setDouble(12, pair.social_distance);
				statement.setString(13, pair.md5);
				statement.executeUpdate();
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
			// Date data format
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
			// Convert input date / time string into seconds in epoch time
			Date dt1 = sdf.parse(args[0]);
			Date dt2 = sdf.parse(args[1]);
			long epoch1 = dt1.getTime();
			long epoch2 = dt2.getTime();
			int ts1 = (int) (epoch1 / 1000);
			int ts2 = (int) (epoch2 / 1000);

			int time_distance = Integer.parseInt(args[2]);
			double social_distance = Double.parseDouble(args[3]);
			// Now do the work. Here we specify the co-location parameters as two devices
			// are within 100 meters in 600 seconds.
			AnalyzeTimeWindow analyze = new AnalyzeTimeWindow(0, ts1, ts2, time_distance, social_distance);
			analyze.start();
			analyze.join();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
