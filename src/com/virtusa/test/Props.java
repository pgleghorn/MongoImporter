package com.virtusa.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Props {

	private static Properties props;

	static {
		props = System.getProperties();
		try {
			props.load(new FileInputStream("MongoImporter.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String get(String propname) {
		return props.getProperty(propname);
	}
	
	public static int getInt(String propname) {
		return Integer.parseInt(get(propname));
	}
	
	public static boolean getBoolean(String propname) {
		return Boolean.parseBoolean(get(propname));
	}

}
