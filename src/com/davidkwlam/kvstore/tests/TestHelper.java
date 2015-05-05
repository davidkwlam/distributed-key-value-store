package com.davidkwlam.kvstore.tests;

import java.nio.ByteBuffer;
import java.util.Random;

import com.davidkwlam.kvstore.Message;

public class TestHelper {

	public static byte[] createRandomId() {
		byte[] id = new byte[Message.ID_BYTES];
		Random r = new Random();
		r.nextBytes(id);
		return id;
	}
	
	public static byte[] createRandomKey() {
		byte[] key = new byte[Message.KEY_BYTES];
		Random r = new Random();
		r.nextBytes(key);
		return key;
	}
	
	public static byte[] createPutRequest(byte[] key, byte[] val) {	
		ByteBuffer buf = ByteBuffer.allocate(Message.MIN_BYTES + Message.KEY_BYTES + Message.VALUE_LENGTH_BYTES + val.length);
		buf.put(createRandomId());		
		buf.put((byte) 0x01); // PUT
		buf.put(key);
		buf.put((byte) (val.length & 0xFF));
		buf.put((byte) ((val.length >> 8) & 0xFF));
		buf.put(val);		
		return buf.array();
	}
	
	public static byte[] createGetRequest(byte[] key) {
		ByteBuffer buf = ByteBuffer.allocate(Message.MIN_BYTES + Message.KEY_BYTES);
		buf.put(createRandomId());
		buf.put((byte) 0x02); // GET
		buf.put(key);
		return buf.array();
	}

	public static byte[] createRemoveRequest(byte[] key) {
		ByteBuffer buf = ByteBuffer.allocate(Message.MIN_BYTES + Message.KEY_BYTES);
		buf.put(createRandomId());		
		buf.put((byte) 0x03); // REMOVE
		buf.put(key);
		return buf.array();
	}
	
	public static byte[] createShutdownRequest(){
		ByteBuffer buf = ByteBuffer.allocate(Message.MIN_BYTES);
		buf.put(createRandomId());		
		buf.put((byte) 0x04); // SHUTDOWN
		return buf.array();
	}
	
	public static byte[] getValue(byte[] response) {
		int startLength = Message.MIN_BYTES;
		int valueLength = (response[startLength] & 0xFF) + ((response[startLength + 1] << 8) & 0xFF00);
		byte[] result = new byte[valueLength];
		
		int startValue = Message.MIN_BYTES + Message.VALUE_LENGTH_BYTES;
		System.arraycopy(response, startValue, result, 0, valueLength);
		return result;
	}

}
