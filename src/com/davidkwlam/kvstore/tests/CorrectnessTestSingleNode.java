package com.davidkwlam.kvstore.tests;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Message;
import com.davidkwlam.kvstore.Utils;

public class CorrectnessTestSingleNode {

//	private static String[] nodes = {"localhost"};
	private static String[] nodes = {"cs-planetlab4.cs.surrey.sfu.ca", "planetlab-01.bu.edu"};
//	private static String[] nodes = {"planetlab-um00.di.uminho.pt", "planetlab1.cs.otago.ac.nz", "planetlab1.jcp-consult.net"};
//	private static String[] nodes = { "planetlab2.ustc.edu.cn" ,"planetlab1.csee.usf.edu"  };
//	private static String node = "planetlab2.cs.uml.edu";
	
	// BAD
//	private static String[] nodes = new String[] {"ple2.ait.ac.th", "planetlab-01.bu.edu",
//		"planetlab-n1.wand.net.nz", "planetlab1.urv.cat", "planetlab2.ci.pwr.wroc.pl"};	
	
//	private static int port = Config.PORT_KVSTORE;
	private static int port = Config.PORT_COORDINATOR;
	
	private static int numKeys = 5;
	
	public static void main(String[] args) throws SocketException, UnknownHostException {
		
		for (String node : nodes) {
			Utils.print("Testing " + node);
			
			ArrayList<byte[]> randomKeys = new ArrayList<>();
			for (int i = 0; i < numKeys; i++) {
				randomKeys.add(TestHelper.createRandomKey());
			}
			
			UdpClient client = new UdpClient(3, 1250);
			
			InetAddress addr = InetAddress.getByName(node);
			
			// PUTS!
			for (byte[] key : randomKeys) {
				byte[] val = ("This is a value for " + Utils.hexString(key)).getBytes();
				byte[] request = TestHelper.createPutRequest(key, val);
				byte[] response = new byte[Message.MIN_BYTES];
				
				long responseTime = client.sendAndReceive(addr, port, request, response);
				
				if (responseTime < Long.MAX_VALUE) {
					Utils.print("PUT response: " + Message.code(response) + " " + responseTime);	
				} else {
					Utils.print("PUT failed (" + addr.getHostName() + ")");
				}
			}
			
			// GETS!
			for (byte[] key : randomKeys) {
				String val = "This is a value for " + Utils.hexString(key);
				byte[] request = TestHelper.createGetRequest(key);
				byte[] response = new byte[Message.RES_MAX_BYTES];
				
				long responseTime = client.sendAndReceive(addr, port, request, response);
				
				if (responseTime < Long.MAX_VALUE) {
					Utils.print("GET response: " + Message.code(response) + " Correct value? " + val.equals(new String(TestHelper.getValue(response))) + " " + responseTime);
				} else {
					Utils.print("GET failed (" + addr.getHostName() + ")");
				}
			}
			
			// REMOVES!
			for (byte[] key : randomKeys) {
				byte[] request = TestHelper.createRemoveRequest(key);
				byte[] response = new byte[Message.MIN_BYTES];
				
				long responseTime = client.sendAndReceive(addr, port, request, response);
				
				if (responseTime < Long.MAX_VALUE) {
					Utils.print("REMOVE response: " + Message.code(response) + " " + responseTime);
				} else {
					Utils.print("REMOVE failed (" + addr.getHostName() + ")");
				}
			}
			
			// GETS!
			for (byte[] key : randomKeys) {			
				byte[] request = TestHelper.createGetRequest(key);
				byte[] response = new byte[Message.MIN_BYTES];
				
				long responseTime = client.sendAndReceive(addr, port, request, response);
				
				if (responseTime < Long.MAX_VALUE) {
					Utils.print("GET response: " + Message.code(response) + " " + responseTime);
				} else {
					Utils.print("GET failed (" + addr.getHostName() + ")");
				}
			}
			
			Utils.print("");
		}		
	}

}
