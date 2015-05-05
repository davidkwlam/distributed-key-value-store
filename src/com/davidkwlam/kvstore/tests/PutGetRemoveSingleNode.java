package com.davidkwlam.kvstore.tests;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Message;
import com.davidkwlam.kvstore.Utils;

public class PutGetRemoveSingleNode {

//	private static String target = "cs-planetlab4.cs.surrey.sfu.ca";
//	private static String target = "planetlab1.tsuniv.edu";
	private static String target = "planetlab3.wail.wisc.edu";
	
	public static void main(String[] args) throws IOException {		
		InetAddress addr = InetAddress.getByName(target);
		
		Random r = new Random();
		
		UdpClient client = new UdpClient(3, 5000);

		ArrayList<Long> putTimes = new ArrayList<>(); 
		ArrayList<Long> getTimes = new ArrayList<>(); 
		ArrayList<Long> remTimes = new ArrayList<>(); 
		
		int count = 1;
		
		while (true) {
			byte[] key = new byte[32];
			byte[] val = new byte[1000]; 
			
			r.nextBytes(key);
			r.nextBytes(val);
	
			byte[] request, response;
			
			// PUT!
			
			request = TestHelper.createPutRequest(key, val);
			response = new byte[Message.RES_MAX_BYTES];
			
			long putTime = client.sendAndReceive(addr, Config.PORT_COORDINATOR, request, response);
			
			if (putTime < Long.MAX_VALUE) {
				putTimes.add(putTime);

				long avgPutTime = putTimes.stream().mapToLong(x -> x).sum() / putTimes.size();
				
				Utils.print(count + "\tPUT: " + Message.code(response) + " " + putTime + "ms (AVG: " + avgPutTime + ") to " + addr.getHostName());
			} else {
				Utils.print(count + "\tPUT failed (" + addr.getHostName() + ")");
				continue;
			}
			
			// GET!
			
			request = TestHelper.createGetRequest(key);
			response = new byte[Message.RES_MAX_BYTES];
			
			long getTime = client.sendAndReceive(addr, Config.PORT_COORDINATOR, request, response);
			
			boolean correctValue = Arrays.equals(val, TestHelper.getValue(response));
			
			if (getTime < Long.MAX_VALUE) {
				getTimes.add(getTime);

				long avgGetTime = getTimes.stream().mapToLong(x -> x).sum() / getTimes.size();
				
				Utils.print(count + "\tGET: " + Message.code(response) + " " + getTime + "ms (AVG: " + avgGetTime + ") to " + addr.getHostName() + (correctValue ? "" : "***********INCORRECT VALUE***********"));
			} else {
				Utils.print(count + "\tGET: failed (" + addr.getHostName() + ")");
			}
			
			// REM!
			
			request = TestHelper.createRemoveRequest(key);
			response = new byte[Message.RES_MAX_BYTES];
			
			long removeTime = client.sendAndReceive(addr, Config.PORT_COORDINATOR, request, response);
			
			if (removeTime < Long.MAX_VALUE) {
				remTimes.add(removeTime);
				
				long avgRemTime = remTimes.stream().mapToLong(x -> x).sum() / remTimes.size();
				
				Utils.print(count + "\tREM: " + Message.code(response) + " " + removeTime + "ms (AVG: " + avgRemTime + ") to "  + addr.getHostName());	
			} else {
				Utils.print(count + "\tREM: failed (" + addr.getHostName() + ")");
			}
			
			count++;
		}
	}

}
