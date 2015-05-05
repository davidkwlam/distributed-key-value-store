package com.davidkwlam.kvstore.tests;

import java.io.IOException;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
//import java.util.ArrayList;
//import java.util.Timer;
//import java.util.TimerTask;
//
//import com.davidkwlam.kvstore.Utils;
//import com.davidkwlam.kvstore.monitor.Node;
//import com.davidkwlam.kvstore.monitor.NodeMonitor;

public class Runner {

//	public static final 
	
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
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
//		String[] SERVERS = new String[] {"cs-planetlab4.cs.surrey.sfu.ca", "planetlab2.cs.uoregon.edu", "planetlab4.cs.uoregon.edu"};
//		Node[] nodes = new Node[SERVERS.length];
//		for (int i = 0; i < nodes.length; i++) {
//			nodes[i] = new Node(SERVERS[i]);
//		}
//		
//		NodeMonitor nm = new NodeMonitor(nodes);
//		
//		new Timer().schedule(new TimerTask() {
//			public void run() {
//				int i = 1;
//				
//				Utils.print("************Available***************");
//				for (Node node : nm.getNodes(false)) {
//					if (node.available()) {
//						Utils.print(i++ + "\t" + node.getAddress().getHostName());	
//					}					
//				}
//				
//				Utils.print("");
//								
//				Utils.print("************Unavailable*************");
//				
//				i = 1;
//				for (Node node : nm.getNodes(false)) {
//					if (!node.available()) {
//						Utils.print(i++ + "\t" + node.getAddress().getHostName());	
//					}					
//				}
//				
//				Utils.print("");
//			}
//		}, 0, 60 * 1000);
	}
}