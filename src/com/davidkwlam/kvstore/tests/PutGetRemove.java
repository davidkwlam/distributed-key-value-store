package com.davidkwlam.kvstore.tests;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Message;
import com.davidkwlam.kvstore.Utils;

public class PutGetRemove {

	private static String[] EXCLUDE = {"planetlab-1.sjtu.edu.cn", "planetlab-2.sjtu.edu.cn", "pl2.zju.edu.cn", "planetlab2.ustc.edu.cn" };
	
	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("Need to supply a file with a list of nodes");
			System.exit(1);
		}

		Set<String> excludeNodes = new HashSet<>();
		Collections.addAll(excludeNodes, EXCLUDE);
		
		// Read in nodes file
		ArrayList<String> hostnames = Utils.readNodesFile(args[0]);
		ArrayList<InetAddress> nodes = new ArrayList<>();
		for (String hostname : hostnames) {
			if (!excludeNodes.contains(hostname)) {
				nodes.add(InetAddress.getByName(hostname));	
			}			
		}
		
		Random r = new Random();
		
		UdpClient client = new UdpClient(3, 5000);

		ArrayList<Long> putTimes = new ArrayList<>(); 
		ArrayList<Long> getTimes = new ArrayList<>(); 
		ArrayList<Long> remTimes = new ArrayList<>(); 
		
		while (true) {
			byte[] key = new byte[32];
			byte[] val = new byte[1000]; 
			
			r.nextBytes(key);
			r.nextBytes(val);
	
			byte[] request, response;
			
			// PUT!
			
			request = TestHelper.createPutRequest(key, val);
			response = new byte[Message.RES_MAX_BYTES];
			
			InetAddress addr = nodes.get(r.nextInt(nodes.size()));
			
			long putTime = client.sendAndReceive(addr, Config.PORT_COORDINATOR, request, response);
			
			if (putTime < Long.MAX_VALUE) {
				putTimes.add(putTime);

				long avgPutTime = putTimes.stream().mapToLong(x -> x).sum() / putTimes.size();
				
				Utils.print("PUT: " + Message.code(response) + " " + putTime + "ms (AVG: " + avgPutTime + ") to " + addr.getHostName());
			} else {
				Utils.print("PUT failed (" + addr.getHostName() + ")");
				continue;
			}
			
			// GET!
			
			request = TestHelper.createGetRequest(key);
			response = new byte[Message.RES_MAX_BYTES];
			
			addr = nodes.get(r.nextInt(nodes.size()));
			
			long getTime = client.sendAndReceive(addr, Config.PORT_COORDINATOR, request, response);
			
			boolean correctValue = Arrays.equals(val, TestHelper.getValue(response));
			
			if (getTime < Long.MAX_VALUE) {
				getTimes.add(getTime);

				long avgGetTime = getTimes.stream().mapToLong(x -> x).sum() / getTimes.size();
				
				Utils.print("GET: " + Message.code(response) + " " + getTime + "ms (AVG: " + avgGetTime + ") to " + addr.getHostName() + (correctValue ? "" : "***********INCORRECT VALUE***********"));
			} else {
				Utils.print("GET: failed (" + addr.getHostName() + ")");
			}
			
			// REM!
			
			request = TestHelper.createRemoveRequest(key);
			response = new byte[Message.RES_MAX_BYTES];
			
			addr = nodes.get(r.nextInt(nodes.size()));
			
			long removeTime = client.sendAndReceive(addr, Config.PORT_COORDINATOR, request, response);
			
			if (removeTime < Long.MAX_VALUE) {
				remTimes.add(removeTime);
				
				long avgRemTime = remTimes.stream().mapToLong(x -> x).sum() / remTimes.size();
				
				Utils.print("REM: " + Message.code(response) + " " + removeTime + "ms (AVG: " + avgRemTime + ") to "  + addr.getHostName());	
			} else {
				Utils.print("REM: failed (" + addr.getHostName() + ")");
			}
		}
	}

}
