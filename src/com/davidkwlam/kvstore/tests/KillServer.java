package com.davidkwlam.kvstore.tests;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Message;
import com.davidkwlam.kvstore.Utils;
import com.davidkwlam.kvstore.Message.Code;
import com.davidkwlam.kvstore.Message.Command;

public class KillServer {
	
	private static String node = "cs-planetlab4.cs.surrey.sfu.ca";
	
	public static void main(String[] args) throws UnknownHostException, SocketException {
		
		UdpClient client = new UdpClient(3, 1250);
		
		InetAddress addr = InetAddress.getByName(node);
		
		byte[] request = Message.createRequest(Command.SHUTDOWN);
		byte[] response = new byte[Message.MIN_BYTES];
		client.sendAndReceive(addr, Config.PORT_COORDINATOR, request, response);
		
		if (Message.code(response) == Code.SUCCESSFUL) {
			Utils.print(node + " killed");	
		}

	}

}
