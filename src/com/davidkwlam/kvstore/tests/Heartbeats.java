package com.davidkwlam.kvstore.tests;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Message;
import com.davidkwlam.kvstore.Utils;
import com.davidkwlam.kvstore.store.StoreMessage;

public class Heartbeats {

	public static void main(String[] args) throws InterruptedException, IOException {
		
		if (args.length < 1) {
			System.out.println("Need to supply a file with a list of nodes");
			System.exit(1);
		}

		// Read in nodes file
		ArrayList<String> hostnames = Utils.readNodesFile(args[0]);
		ArrayList<InetAddress> nodes = new ArrayList<>();
		for (String hostname : hostnames) {
//			try {
				nodes.add(InetAddress.getByName(hostname));
//			} catch (UnknownHostException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}

		UdpClient client = new UdpClient(3, 1250);
		
		Random r = new Random();
		
		while (true) {
			Utils.print("Pinging coordinators...");
			
			byte[] key = new byte[32];
			r.nextBytes(key);
			
			byte[] request = TestHelper.createGetRequest(key);
			
			for (InetAddress node : nodes) {
			
				byte[] response = new byte[Message.RES_MAX_BYTES];

				long responseTime = client.sendAndReceive(node, Config.PORT_COORDINATOR, request, response);
				
				if (responseTime < Long.MAX_VALUE) {
//					Utils.print("HEARTBEAT: " + " " + responseTime + "ms\t"+ node.getHostName());	
				} else {
					Utils.print("HEARTBEAT: " + " FAILED\t"+ node.getHostName());
				}
				
			}
			
			Utils.print("");
			
			Utils.print("Pinging stores...");
			
			request = StoreMessage.createHeartbeatRequest();
			
			for (InetAddress node : nodes) {
				
				byte[] response = new byte[StoreMessage.MAX_RES_BYTES];

				long responseTime = client.sendAndReceive(node, Config.PORT_COORDINATOR, request, response);
				
				if (responseTime < Long.MAX_VALUE) {
//					Utils.print("HEARTBEAT: " + " " + responseTime + "ms\t"+ node.getHostName());	
				} else {
					Utils.print("HEARTBEAT: " + " FAILED\t"+ node.getHostName());
				}
				
			}
			
			Utils.print("");
			
			Thread.sleep(60 * 1000);
		}
		
	}

}
