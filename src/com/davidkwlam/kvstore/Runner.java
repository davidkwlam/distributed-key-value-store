package com.davidkwlam.kvstore;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.davidkwlam.kvstore.coordinator.Coordinator;
import com.davidkwlam.kvstore.coordinator.Node;
import com.davidkwlam.kvstore.coordinator.Monitor;
import com.davidkwlam.kvstore.store.Store;
import com.davidkwlam.kvstore.store.StoreClient;

public class Runner {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Need to supply a file with a list of nodes");
			System.exit(1);
		}
		
		ExecutorService executor = Executors.newCachedThreadPool();
		
		Store store = new Store(Config.PORT_STORE);
		new Thread(() -> store.serve(executor)).start();
		
		Utils.print("Store serving on port " + Config.PORT_STORE + "...");
		
		ArrayList<String> hostnames = Utils.readNodesFile(args[0]);

		Node[] nodes = new Node[hostnames.size()];

		for (int i = 0; i < hostnames.size(); i++) {
			 // We want this to exit if any host is unresolvable
			InetAddress addr = InetAddress.getByName(hostnames.get(i));
			boolean localhost = addr.equals(InetAddress.getLocalHost());
			nodes[i] = new Node(addr, localhost);
		}
		
		StoreClient client = new StoreClient(Config.PORT_STORE, 5, 10000);
		new Thread(() -> client.receive(executor)).start();
		
		Monitor monitor = new Monitor(nodes, client);
		
		Coordinator coordinator = new Coordinator(Config.PORT_COORDINATOR, monitor, client);
		new Thread(() -> coordinator.serve(executor)).start();
		
		Utils.print("Coordinator serving on port " + Config.PORT_COORDINATOR + "...");		
	}

}
