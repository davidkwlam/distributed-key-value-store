package com.davidkwlam.kvstore.tests;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Message;
import com.davidkwlam.kvstore.Utils;

public class LoadGenerator {

//	private static String node = "cs-planetlab4.cs.surrey.sfu.ca";
	private static String node = "planetlab2.cs.colorado.edu";
	
	public static void main(String[] args) throws SocketException, UnknownHostException {
		
		DatagramSocket socket = new DatagramSocket();
		socket.setSoTimeout(5000);
		
		InetAddress addr = InetAddress.getByName(node);
		
		ArrayList<byte[]> randomKeys = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			randomKeys.add(TestHelper.createRandomKey());
		}
		
		// PUTS!
		for (byte[] key : randomKeys) {
			byte[] val = ("This is a value for " + Utils.hexString(key)).getBytes();
			
			byte[] putRequest = TestHelper.createPutRequest(key, val);
			
			DatagramPacket p = new DatagramPacket(putRequest, putRequest.length, addr, Config.PORT_COORDINATOR);
			
			try {
				long start = System.currentTimeMillis();	
				
				socket.send(p);
				
				byte[] response = new byte[Message.RES_MAX_BYTES];
				p.setData(response);
				socket.receive(p);
				
				long end = System.currentTimeMillis();
				
				Utils.print("Put response: " + Message.code(response) + " " + (end - start));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// GETS!
		boolean running = true;
		
		while (running) {
			
			for (byte[] key : randomKeys) {
				String val = "This is a value for " + Utils.hexString(key);
				
				byte[] getRequest = TestHelper.createGetRequest(key);
				
				DatagramPacket p = new DatagramPacket(getRequest, getRequest.length, addr, Config.PORT_COORDINATOR);
				
				try {
					long start = System.currentTimeMillis();	
					
					socket.send(p);
					
					byte[] response = new byte[Message.RES_MAX_BYTES];
					p.setData(response);
					socket.receive(p);
					
					long end = System.currentTimeMillis();
					
					Utils.print("GET response: " + Message.code(response) + " Correct value? " + val.equals(new String(TestHelper.getValue(response))) + " " +(end - start));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		}
		
		socket.close();
	}

}
