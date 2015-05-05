package com.davidkwlam.kvstore.store;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.davidkwlam.kvstore.Utils;

public class StoreClient {

	private final int _port;
	private final int _attempts;
	private final int _timeoutMs;
	private final DatagramSocket _socket;
	private final ScheduledExecutorService _executor = Executors.newSingleThreadScheduledExecutor();
	private final ConcurrentHashMap<String, Consumer<byte[]>> _onSuccess = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, Consumer<byte[]>> _onFailure = new ConcurrentHashMap<>();
	
	public StoreClient(int port, int attempts, int timeoutMs) throws SocketException {
		_port = port;
		_attempts = attempts;
		_timeoutMs = timeoutMs;
		_socket = new DatagramSocket();
		_socket.setReuseAddress(true);
	}

	public void send(InetAddress addr, byte[] data, Consumer<byte[]> onSuccess, Consumer<byte[]> onFailure) {
		String key = key(addr, _port, StoreMessage.id(data));

		_onSuccess.put(key, onSuccess);
		_onFailure.put(key, onFailure);
		
		DatagramPacket packet = new DatagramPacket(data, data.length, addr, _port);
		
		// Send with retries
		for (int i = 0; i < _attempts; i++) {
			_executor.schedule(() -> {
				if (_onSuccess.containsKey(key)) {
					try {
						_socket.send(packet);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}, (int) (i == 0 ? 0 : 250 * Math.pow(2, i)) , TimeUnit.MILLISECONDS);
		}
		 
		// Give up if no response received
		_executor.schedule(() -> {
			Consumer<byte[]> thisOnSuccess = _onSuccess.remove(key);
			Consumer<byte[]> thisOnFailure = _onFailure.remove(key);
			
			if (thisOnSuccess != null) {
				thisOnFailure.accept(data);
			}
		}, _timeoutMs, TimeUnit.MILLISECONDS);
	}
	
	public void receive(ExecutorService executor) {
		while (true) {
			byte[] response = new byte[StoreMessage.MAX_RES_BYTES];
			DatagramPacket packet = new DatagramPacket(response, response.length);

			try {
				_socket.receive(packet);
				
				executor.execute(() -> {
					String key = key(packet.getAddress(), packet.getPort(), StoreMessage.id(response));
					
					Consumer<byte[]> onSuccess = _onSuccess.remove(key);
					
					if (onSuccess != null) {
						onSuccess.accept(response);
						_onFailure.remove(key);
					}
				});
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static String key(InetAddress addr, int port, byte[] requestId) {
		return Utils.hexString(addr.getAddress()) + Integer.toHexString(port) + Utils.hexString(requestId); 
	}
	
}
