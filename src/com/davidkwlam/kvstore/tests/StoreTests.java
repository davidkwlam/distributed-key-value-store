package com.davidkwlam.kvstore.tests;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Utils;
import com.davidkwlam.kvstore.store.StoreMessage;

public class StoreTests {
	
	private static String node = "cs-planetlab4.cs.surrey.sfu.ca";
//	private static String node = "planetlab2.cs.colorado.edu";
//	private static String node = "planetlab2.cs.uml.edu";	
//	private static String node = "planetlab3.xeno.cl.cam.ac.uk";	

	public static void main(String[] args) throws SocketException, UnknownHostException {
		
		UdpClient client = new UdpClient(5, 1000);
		
		InetAddress addr = InetAddress.getByName(node);
		
		byte[] key = "12345678".getBytes();
		byte[] val = "This is a test value".getBytes();
//		byte[] val = new byte[0];
		
		byte[] req, res;
		
		byte[] id = StoreMessage.createId();
		
		// PUT
		req = StoreMessage.createPutRequest(id, key, val, 0);
		res = new byte[StoreMessage.MAX_RES_BYTES];

		client.sendAndReceive(addr, Config.PORT_STORE, req, res);
		
		Utils.print("PUT: " + StoreMessage.storeResponseType(res));
		
		// GET
		req = StoreMessage.createGetRequest(id, key);
		res = new byte[StoreMessage.MAX_RES_BYTES];
		
		client.sendAndReceive(addr, Config.PORT_STORE, req, res);
		
		Utils.print("GET: " + StoreMessage.storeResponseType(res) 
				+ "; VERSION: " + StoreMessage.responseValueVersion(res)
				+ "; VALUE LENGTH: " + StoreMessage.responseValue(res).length
				+ "; VALUE: " + new String(StoreMessage.responseValue(res)));
		
	}
	
}
