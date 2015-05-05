package com.davidkwlam.kvstore.tests;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Message;
//import com.davidkwlam.kvstore.Message.Code;
import com.davidkwlam.kvstore.Utils;
import com.davidkwlam.kvstore.Message.Code;

public class CorrectnessTest {

	private static String[] EXCLUDE = { "planetlab1.tsuniv.edu", "planetlab1.u-strasbg.fr" , "planetlab-1.sjtu.edu.cn" };

	private static int port = Config.PORT_COORDINATOR;
	
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

		// Generate random keys
		ArrayList<byte[]> randomKeys = new ArrayList<>();
		for (int i = 0; i < 500; i++) {
			randomKeys.add(TestHelper.createRandomKey());
		}

		Random r = new Random();
		
		Utils.print("Starting puts...");
		
		HashMap<InetAddress, Integer> _attempts = new HashMap<>();
		HashMap<InetAddress, Integer> _failures = new HashMap<>();
		for (InetAddress addr: nodes) {
			_attempts.put(addr, 0);
			_failures.put(addr, 0);
		}
		
		ArrayList<Long> _responseTimes = new ArrayList<>();
		
		UdpClient client = new UdpClient(3, 1250);
		
		int count = 0;
		
		HashSet<byte[]> failedKeys = new HashSet<>();
		
		// PUTS!
		for (byte[] key : randomKeys) {
			byte[] val = ("This is a value for " + Utils.hexString(key)).getBytes();
			byte[] request = TestHelper.createPutRequest(key, val);
			byte[] response = new byte[Message.RES_MAX_BYTES];

			InetAddress addr = nodes.get(r.nextInt(nodes.size()));
			
			_attempts.put(addr, _attempts.get(addr) + 1);
			long responseTime = client.sendAndReceive(addr, port, request, response);
			
			if (responseTime < Long.MAX_VALUE) {				
				Utils.print(count + "\tPUT: " + Message.code(response) + " " + responseTime + "ms to " + addr.getHostName());
				_responseTimes.add(responseTime);
			} else {
				Utils.print(count + "\tPUT: *****FAILED***** (" + addr.getHostName() + ")");
				_failures.put(addr, _failures.get(addr) + 1);
				failedKeys.add(key);
			}
			
			count++;
		}

		count = 0;
		
		// GETS!
		for (byte[] key : randomKeys) {
			if (!failedKeys.contains(key)) {
				String val = "This is a value for " + Utils.hexString(key);
				byte[] request = TestHelper.createGetRequest(key);
				byte[] response = new byte[Message.RES_MAX_BYTES];
				
				InetAddress addr = nodes.get(r.nextInt(nodes.size()));
				_attempts.put(addr, _attempts.get(addr) + 1);
				
				long responseTime = client.sendAndReceive(addr, port, request, response);
				
				if (responseTime < Long.MAX_VALUE) {
					Utils.print(count + "\tGET: " + Message.code(response) + " Correct value? " + val.equals(new String(TestHelper.getValue(response))) + " " + responseTime + "ms to " + addr.getHostName());
					_responseTimes.add(responseTime);
					
					if (Message.code(response) == Code.NON_EXISTENT_KEY) {
						Utils.print("KEY: " + Utils.hexString(key));
					}
					
				} else {
					Utils.print(count + "\tGET: *****FAILED***** (" + addr.getHostName() + ")");
					_failures.put(addr, _failures.get(addr) + 1);
				}	
			} else {
				Utils.print(count + "\tGET: Skipped due to failed put");
			}
			
			count++;
		}

		Utils.print("");
		
		for (InetAddress addr : nodes) {
			if (_failures.get(addr) > 0) {
				Utils.print("Failure rate: " + _failures.get(addr) + "/" + _attempts.get(addr) + "\t" + addr.getHostName());	
			}
		}
		
		Utils.print("");
		
		for (InetAddress addr : nodes) {
			if (_attempts.get(addr) == 0) {
				Utils.print("No attempts: " + addr.getHostName());	
			}
		}
		
		Utils.print("");
		
		long total = 0;
		for (Long l : _responseTimes) {
			total += l;
		}
		long avgResponseTime = total / _responseTimes.size();
		
		Utils.print("Average response time: " + avgResponseTime);
	}

}
