package com.davidkwlam.kvstore.tests;

import java.io.IOException;
//import java.net.DatagramPacket;
//import java.net.DatagramSocket;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.ArrayList;

//import com.davidkwlam.kvstore.Config;
//import com.davidkwlam.kvstore.Message;
//import com.davidkwlam.kvstore.Utils;
//import com.davidkwlam.kvstore.Message.Code;
//import com.davidkwlam.kvstore.Message.Command;
//import com.davidkwlam.kvstore.monitor.Node;

public class ShutdownKVStores {

	public static void main(String[] args) throws IOException {
//		if (args.length < 1) {
//			System.out.println("Need to supply a file with a list of nodes");
//			System.exit(1);
//		}
//		
//		ArrayList<String> hostnames = Utils.readNodesFile(args[0]);
//		
//		Node[] nodes = new Node[hostnames.size()];
//		
//		for (int i = 0; i < hostnames.size(); i++) {
//			InetAddress addr = null;
//			boolean localhost = false;
//			try {
//				addr = InetAddress.getByName(hostnames.get(i));
//				localhost = addr.equals(InetAddress.getLocalHost());
//			} catch (UnknownHostException e) {
//				Utils.print("Unresolvable: " + hostnames.get(i));
//				e.printStackTrace();
//			}
//			nodes[i] = new Node(addr, localhost); // TODO announce failure to rest of group
//		}
//		
//		DatagramSocket socket = new DatagramSocket();
//		socket.setSoTimeout(2000);
//		
//		byte[] req = Message.createRequest(Command.SHUTDOWN);
//		
//		UdpClient client = new UdpClient(3, 1250);
//		
//		for (Node node : nodes) {
//			try {
//				byte[] request = Message.createRequest(Command.SHUTDOWN);
//				byte[] response = new byte[Message.MIN_BYTES];
//				client.sendAndReceive(node.getAddress(), Config.PORT_SERVER, request, response);
//				Utils.print(node.getAddress().getHostName() + " killed");
//			} catch (Exception e) {
//				e.printStackTrace();
//				continue;
//			}
//			
//			
//			if (node.available()) {
//				try {
//					boolean done = false;
//					while (!done) {
//						socket.send(new DatagramPacket(req, req.length, node.getAddress(), Config.PORT_SERVER));
//						
//						byte[] res = new byte[Message.MIN_BYTES];
//						DatagramPacket packet = new DatagramPacket(res, res.length);
//						
//						try {
//							socket.receive(packet);
//							if (packet.getAddress().equals(node.getAddress()) 
//									&& Message.code(res) == Code.SUCCESSFUL) {
//								done = true;
//							}
//						} catch (IOException e) {
//							e.printStackTrace();
//						}
//					}						
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		
//		socket.close();
	}

}
