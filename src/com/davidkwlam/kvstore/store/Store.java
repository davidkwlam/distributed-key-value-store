package com.davidkwlam.kvstore.store;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

//import com.davidkwlam.kvstore.Config;
import com.davidkwlam.kvstore.Utils;
import com.davidkwlam.kvstore.store.StoreMessage.StoreResponseType;

public class Store {

	private final DatagramSocket _socket;
	private final ConcurrentHashMap<String, Value> _store = new ConcurrentHashMap<String, Value>();
	private final ConcurrentHashMap<String, byte[]> _responseCache = new ConcurrentHashMap<String, byte[]>();	
	
	public Store(int port) throws SocketException {
		_socket = new DatagramSocket(port);
		_socket.setReuseAddress(true);
	}
	
	public void serve(ExecutorService executor) {
		while (true) {
			byte[] request = new byte[StoreMessage.MAX_REQ_BYTES];
			DatagramPacket packet = new DatagramPacket(request, request.length);

			try {
				_socket.receive(packet);
				
				executor.execute(() -> {
					byte[] id = StoreMessage.id(request);

					byte[] key = StoreMessage.key(request);

					byte[] response = _responseCache.get(Utils.hexString(id));

					if (response == null) {
						switch (StoreMessage.storeRequestType(request)) {
							case PUT:
								response = put(id, key, StoreMessage.requestValue(request), StoreMessage.requestValueVersion(request));
								break;
							case GET:
								response = get(id, key);
								break;
							case HEARTBEAT_REQ:
								response = StoreMessage.createResponse(id, StoreResponseType.HEARTBEAT_ACK);
								break;
							default:
								response = StoreMessage.createResponse(id, StoreResponseType.UNRECOGNIZED_COMMAND);
								break;
						}
						
						_responseCache.put(Utils.hexString(id), response);
					}

					packet.setData(response);
					
					try {
						_socket.send(packet);
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private byte[] get(byte[] id, byte[] key) {		
		String keyString = Utils.hexString(key);
		
		if (!_store.containsKey(keyString)) {
			_store.put(keyString, new Value(new byte[0], 0));
		}
		
		Value currVal = _store.get(keyString);
		
		StoreResponseType response = currVal.value.length > 0 ? StoreResponseType.SUCCESSFUL : StoreResponseType.NON_EXISTENT_KEY;
		
		return StoreMessage.createResponse(id, response, currVal.version, currVal.value);
	}

	private byte[] put(byte[] id, byte[] key, byte[] val, int ver) {
//		if (_store.size() > Config.KVSTORE_SIZE) {
//			return StoreMessage.createResponse(id, StoreResponseType.OUT_OF_SPACE);
//		}
		
		String keyString = Utils.hexString(key);
		
		if (ver > 0) { // this is a repair
			_store.put(keyString, new Value(val, ver));	
			return StoreMessage.createResponse(id, StoreResponseType.SUCCESSFUL);
		} 

		if (!_store.containsKey(keyString)) {
			_store.put(keyString, new Value(new byte[0], 0));
		}
		
		Value currVal = _store.get(keyString);
			
		if (currVal.value.length == 0 && val.length == 0) { // a REMOVE for a non-existent key-value
			return StoreMessage.createResponse(id, StoreResponseType.NON_EXISTENT_KEY);
		}
		
		_store.put(keyString, new Value(val, currVal.version + 1));	

		return StoreMessage.createResponse(id, StoreResponseType.SUCCESSFUL);
	}

	class Value {
		final byte[] value;
		final int version;
		
		Value(byte[] val, int ver) {
			value = val;
			version = ver;
		}
	}
}
