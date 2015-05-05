package com.davidkwlam.kvstore.coordinator;

import java.math.BigInteger;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.store.StoreClient;
import com.davidkwlam.kvstore.store.StoreMessage;

public class Monitor {

	private final Node[] _nodes;
	private final MessageDigest _digest;

	public Monitor(Node[] nodes, StoreClient client) throws SocketException, NoSuchAlgorithmException {
		_nodes = nodes;	
		_digest = MessageDigest.getInstance(Config.HASHING_ALGORITHM);

		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
				() -> {
					for (Node node : _nodes) {
						client.send(node.getAddress(), StoreMessage.createHeartbeatRequest(),
								res -> node.setAvailable(true), req -> node.setAvailable(false));
					}
				}, 0, 1, TimeUnit.MINUTES);
	}

	public Node[] getNodes(boolean onlyAvailable) {
		if (onlyAvailable) {
			ArrayList<Node> available = new ArrayList<>();
			for (Node node : _nodes) {
				if (node.available()) {
					available.add(node);
				}
			}
			return available.toArray(new Node[0]);
		}
		return _nodes;
	}
	
	public int getId(byte[] data) {
		_digest.update(data);
		BigInteger bigInt = new BigInteger(_digest.digest());
		return bigInt.intValue() & Integer.MAX_VALUE % _nodes.length;
	}
	
	/*
	 * Gets all the successors for this key, until numAvailable available nodes
	 * are retrieved.
	 */
	public Set<Node> getSuccessors(byte[] key, int numAvailable) {
		int id = getId(key);
		
		if (id < 0 || numAvailable < 1 || _nodes.length < 1) {
			return new HashSet<Node>();
		}

		HashSet<Node> successors = new HashSet<>();
		
		int availableNodes = 0;
		
		while (availableNodes < numAvailable) {
			Node node = _nodes[id % _nodes.length];
			
			successors.add(node);
			
			if (node.available()) {
				availableNodes++;
			}

			id++;
		}
		
		return successors;
	}

//	public Node[] getSuccessors(int id, int numSuccessors, boolean excludeUnavailable) {
//		if (id < 0 || numSuccessors < 1 || _nodes.length < 1) {
//			return new Node[0];
//		}
//
//		Node[] successors = new Node[numSuccessors];
//
//		for (int i = 0; i < successors.length; i++) {
//			if (excludeUnavailable) {
//				while (!_nodes[id % _nodes.length].available()) {
//					id++;
//				}
//			}
//
//			successors[i] = _nodes[id % _nodes.length];
//
//			id++;
//		}
//
//		return successors;
//	}
	
}
