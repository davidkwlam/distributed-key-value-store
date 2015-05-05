package com.davidkwlam.kvstore.tests;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Random;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Message;
import com.davidkwlam.kvstore.Utils;
import com.davidkwlam.kvstore.Message.Code;
import com.davidkwlam.kvstore.Message.Command;

public class CatastrophicFailure {

private static int port = Config.PORT_COORDINATOR;
	
	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("Need to supply a file with a list of nodes");
			System.exit(1);
		}

		// Read in nodes file
		ArrayList<String> hostnames = Utils.readNodesFile(args[0]);
		ArrayList<InetAddress> nodes = new ArrayList<>();
		for (String hostname : hostnames) {
			nodes.add(InetAddress.getByName(hostname));
		}

		// Generate random keys
		ArrayList<byte[]> randomKeys = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			randomKeys.add(TestHelper.createRandomKey());
		}

		Random r = new Random();
		
		Utils.print("Starting...");
		
		ArrayList<Long> responseTimes = new ArrayList<>();
		ArrayList<String> failedKeys = new ArrayList<>();
		
		UdpClient client = new UdpClient(3, 1250);
		
		// PUTS!
		for (byte[] key : randomKeys) {
			byte[] val = ("This is a value for " + Utils.hexString(key)).getBytes();
			byte[] request = TestHelper.createPutRequest(key, val);
			byte[] response = new byte[Message.RES_MAX_BYTES];

			InetAddress addr = nodes.get(r.nextInt(nodes.size()));
			
			long responseTime = client.sendAndReceive(addr, port, request, response);
			
			if (responseTime < Long.MAX_VALUE) {
				Utils.print("PUT response: " + Message.code(response) + " " + responseTime + "ms to " + addr.getHostName());
				responseTimes.add(responseTime);
			} else {
				Utils.print("PUT failed (" + addr.getHostName() + ")");
				failedKeys.add(Utils.hexString(key));
			}
		}
		
		// SHUTDOWNS!
		
		// Fail 20% of nodes
		int numToFail = nodes.size() / 5;
		
		while (numToFail > 0) {
			try {
				byte[] request = Message.createRequest(Command.SHUTDOWN);
				byte[] response = new byte[Message.MIN_BYTES];
				
				InetAddress nodeToDie = nodes.remove(r.nextInt(nodes.size()));
				
				client.sendAndReceive(nodeToDie, port, request, response);
				
				if (Message.sameId(request, response) && Message.code(response) == Code.SUCCESSFUL) {
					Utils.print(nodeToDie.getHostName() + " killed");
				} else {
					Utils.print(nodeToDie.getHostName() + " killed??????? Dunno!");
				}
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
			numToFail--;
		}		

		// Wait 3 minutes
		try {
			Thread.sleep(3 * 60 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		

		int attempts = 0;
		int successes = 0;
		
		// GETS!
		for (byte[] key : randomKeys) {
			if (failedKeys.contains(Utils.hexString(key))) {
				continue;
			}
			
			attempts++;
			
			String val = "This is a value for " + Utils.hexString(key);
			byte[] request = TestHelper.createGetRequest(key);
			byte[] response = new byte[Message.RES_MAX_BYTES];
			
			InetAddress addr = nodes.get(r.nextInt(nodes.size()));
			
			long responseTime = client.sendAndReceive(addr, port, request, response);
			
			if (responseTime < Long.MAX_VALUE) {
				if (Message.code(response) == Code.SUCCESSFUL) {
					if (val.equals(new String(TestHelper.getValue(response)))) {
						Utils.print("GET response: " + Message.code(response) + " with correct value "+ responseTime + "ms to " + addr.getHostName());
						successes++;
					} else {
						Utils.print("GET response: " + Message.code(response) + " with INCORRECT value "+ responseTime + "ms to " + addr.getHostName());
					}
				} else {
					Utils.print("GET response: " + Message.code(response) + " " + responseTime + "ms to " + addr.getHostName() + " for key " + Utils.hexString(key));
				}
				responseTimes.add(responseTime);
			} else {
				Utils.print("GET failed (" + addr.getHostName() + ")");
			}
		}

		Utils.print("");
		
		Utils.print("Success rate: " + successes + "/" + attempts);
		
		long total = 0;
		for (Long l : responseTimes) {
			total += l;
		}
		long avgResponseTime = total / responseTimes.size();
		
		Utils.print("");
		
		Utils.print("Average response time: " + avgResponseTime);
	}
	
}
