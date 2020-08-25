package net.qyjohn.simx;

import java.security.MessageDigest;
import java.math.BigInteger;

public class DataPair
{
	DataPoint p1, p2;
	int    time_distance;
	double social_distance;
	String md5;
	
	public DataPair(DataPoint p1, DataPoint p2, int time_distance, double social_distance)
	{
		this.p1 = p1;
		this.p2 = p2;
		this.time_distance   = time_distance;
		this.social_distance = social_distance;

		try 
		{
			String check = p1.uid + p1.ds + p2.uid + p2.ds;
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] digest = md.digest(check.getBytes());
			BigInteger bigInt = new BigInteger(1,digest);
			String hashtext = bigInt.toString(16);
			// Now we need to zero pad it if you actually want the full 32 chars.
			while(hashtext.length() < 32 )
			{
				hashtext = "0"+hashtext;
			}
			this.md5 = hashtext;
		} catch(Exception e) 
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
