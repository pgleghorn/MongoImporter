package com.virtusa.test;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;

public class Log {

	static File logfile;
	
	static {
		logfile = new File(Props.get("logfile"));
	}
	
	public static void print(String msg) {
		Date now = new Date();
		String logmsg = now + ": " + msg;
		System.out.println(logmsg);
		try {
			FileUtils.writeStringToFile(logfile, logmsg + "\n", true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
