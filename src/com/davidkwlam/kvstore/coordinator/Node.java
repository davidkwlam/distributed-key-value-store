package com.davidkwlam.kvstore.coordinator;

import java.net.InetAddress;

import com.davidkwlam.kvstore.Utils;

public class Node {

	private InetAddress _address;
	private boolean _available;
	private boolean _localhost;
	
	public Node(InetAddress addr, boolean localhost) {
		_address = addr;
		_localhost = localhost;
		_available = false;
	}
	
	public InetAddress getAddress() {
		return _address;
	}
	
	public boolean localhost() {
		return _localhost;
	}
	
	public boolean available() {
		return _available;
	}

	public void setAvailable(boolean val) {
		if (_available != val) {
			_available = val;
			
			Utils.print((val ? "UP: " : "DOWN: ") + _address.getHostName());
		}
	}
}
