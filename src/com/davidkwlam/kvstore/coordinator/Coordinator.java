package com.davidkwlam.kvstore.coordinator;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Message;
import com.davidkwlam.kvstore.Utils;
import com.davidkwlam.kvstore.Message.Code;
import com.davidkwlam.kvstore.Message.Command;
import com.davidkwlam.kvstore.store.StoreClient;
import com.davidkwlam.kvstore.store.StoreMessage;

public class Coordinator {

	private final DatagramSocket _socket;
	private final Monitor _monitor;
	private final StoreClient _client;
	
	private final ConcurrentHashMap<String, byte[]> _responseCache = new ConcurrentHashMap<String, byte[]>();
	private final ConcurrentHashMap<Command, Consumer<DatagramPacket>> _handlers = new ConcurrentHashMap<Command, Consumer<DatagramPacket>>();
	
	public Coordinator(int port, Monitor monitor, StoreClient client) throws SocketException {
		_socket = new DatagramSocket(port);
		_socket.setReuseAddress(true);
		_monitor = monitor;
		_client = client;

		_handlers.put(Command.SHUTDOWN, this::shutdown);
		_handlers.put(Command.PUT, this::put);
		_handlers.put(Command.REMOVE, this::remove);
		_handlers.put(Command.GET, this::get);
	}
	
	public void serve(ExecutorService executor)  {		
		while (true) {
			byte[] request = new byte[Message.REQ_MAX_BYTES];
			DatagramPacket packet = new DatagramPacket(request, request.length);

			try {
				_socket.receive(packet);		
				
				executor.execute(() -> {
					byte[] id = Message.id(request);
						
					byte[] response = _responseCache.get(Utils.hexString(id));
					
					if (response != null) {
						if (response.length > 0) {
							send(packet, response);	
						}
					} else {
						_responseCache.put(Utils.hexString(id), new byte[0]);
						
						Command command = Message.command(request);
						
						if (_handlers.containsKey(command)) {
							_handlers.get(command).accept(packet);
						} else {
							send(packet, Message.createResponse(id, Code.UNRECOGNIZED_COMMAND));
						}
					}
				});
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}	

	private void shutdown(DatagramPacket packet) {
		byte[] request = packet.getData();
		byte[] id = Message.id(request);
		
		send(packet, Message.createResponse(id, Code.SUCCESSFUL));
		
		System.exit(1);
	}
	
	private void put(DatagramPacket packet) {
		byte[] request = packet.getData();
		byte[] id = Message.id(request);
		byte[] key = Message.key(request);
		byte[] val = Message.requestValue(request);

		put(packet, id, key, val);
	}

	private void remove(DatagramPacket packet) {
		byte[] request = packet.getData();
		byte[] id = Message.id(request);
		byte[] key = Message.key(request);

		put(packet, id, key, new byte[0]);
	}
	
	private void put(DatagramPacket packet,  byte[] id, byte[] key, byte[] val) {
		String requestId = Utils.hexString(id);
		
		byte[] storeRequest = StoreMessage.createPutRequest(id, key, val, 0);
		
		HashMap<Node, byte[]> responses = new HashMap<Node, byte[]>();
		
		for (Node node : _monitor.getSuccessors(key, Config.REPLICATION_FACTOR)) {
			Consumer<byte[]> onSuccess = res -> {		
				node.setAvailable(true);
				
				synchronized (responses) {
					if (_responseCache.containsKey(requestId) && _responseCache.get(requestId).length == 0) {
						int votes = 1;
						
						for (byte[] otherRes : responses.values()) {
							if (StoreMessage.storeResponseType(res) 
									== StoreMessage.storeResponseType(otherRes)) {
								votes++;
							}
						}

						if (votes >= Config.WRITE_QUORUM) { // Winner, winner, chicken dinner!
							Code responseCode;
							
							switch (StoreMessage.storeResponseType(res)) {
								case SUCCESSFUL: 
									responseCode = Code.SUCCESSFUL;
									break;
								case NON_EXISTENT_KEY:
									responseCode = Code.NON_EXISTENT_KEY;
									break;
								case OUT_OF_SPACE:
									responseCode = Code.OUT_OF_SPACE;
									break;
								default:
									responseCode = Code.INTERNAL_FAILURE;
									break;
							}
							
							byte[] response = Message.createResponse(id, responseCode);
							
							_responseCache.put(requestId, response);
							send(packet, response);
						}
					}
					
					responses.put(node, res);
				}
			};
			
			Consumer<byte[]> onFailure = req -> {
				node.setAvailable(false);
			};
			
			_client.send(node.getAddress(), storeRequest, onSuccess, onFailure);
		}
	}
	
	private void get(DatagramPacket packet) {
		byte[] request = packet.getData();
		byte[] id = Message.id(request);
		byte[] key = Message.key(request);
		
		String requestId = Utils.hexString(id);

		byte[] storeRequest = StoreMessage.createGetRequest(id, key);
				
		HashMap<Node, byte[]> responses = new HashMap<Node, byte[]>();
		
		Set<Node> nodes = _monitor.getSuccessors(key, Config.REPLICATION_FACTOR);
		
		for (Node node : nodes) {
			Consumer<byte[]> onSuccess = res -> {
				node.setAvailable(true);
				
				synchronized (responses) {
					if (_responseCache.containsKey(requestId) && _responseCache.get(requestId).length == 0) {
						if (responses.size() + 1 >= Config.READ_QUORUM) {
							byte[] finalResponse = res;
							
							for (byte[] otherRes : responses.values()) {
								if (StoreMessage.responseValueVersion(otherRes) 
										> StoreMessage.responseValueVersion(finalResponse)) {
									finalResponse = otherRes;
								}
							}
							
							byte[] value = StoreMessage.responseValue(finalResponse); 
							
							byte[] response = value.length > 0 ? 
									Message.createResponse(id, Code.SUCCESSFUL, value) : 
									Message.createResponse(id, Code.NON_EXISTENT_KEY);
					
							_responseCache.put(requestId, response);
							send(packet, response);		
						}
					}
					
					responses.put(node, res);
					
					// Repair when all responses have been received
					if (responses.keySet().containsAll(nodes)) {
						repair(key, responses);	
					}
				}
			};
			
			Consumer<byte[]> onFailure = req -> {
				node.setAvailable(false);
			};
			
			_client.send(node.getAddress(), storeRequest, onSuccess, onFailure);
		}
	}
	
	private void repair(byte[] key, HashMap<Node, byte[]> responses) {
		byte[] value = null;
		int version = 0;
		
		boolean needsRepairing = false;
		
		for (byte[] res : responses.values()) {
			int resVersion = StoreMessage.responseValueVersion(res);
			
			if (resVersion > version) {
				if (value != null) {
					needsRepairing = true;
				}
				
				value = StoreMessage.responseValue(res);
				version = resVersion;
			}
		}

		if (needsRepairing) {
			byte[] repairRequest = StoreMessage.createPutRequest(StoreMessage.createId(), key, value, version);

			for (Node node : responses.keySet()) {		
				Consumer<byte[]> onSuccess = res -> {
					node.setAvailable(true);
				};
				
				Consumer<byte[]> onFailure = RequestingUserName -> {
					node.setAvailable(false);
				};
				
				_client.send(node.getAddress(), repairRequest, onSuccess, onFailure);
			}
		}
	}
	
	private void send(DatagramPacket packet, byte[] data) {
		try {
			packet.setData(data);
			_socket.send(packet);
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
}
