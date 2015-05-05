package com.davidkwlam.kvstore;

import java.util.Arrays;
import java.util.Random;

public class Message {

	public enum Command {
		PUT, GET, REMOVE, SHUTDOWN, HEARTBEAT_REQ, UNKNOWN
	}
	
	public enum Code {
		SUCCESSFUL, NON_EXISTENT_KEY, OUT_OF_SPACE, SYSTEM_OVERLOAD, INTERNAL_FAILURE, UNRECOGNIZED_COMMAND, HEARTBEAT_ACK
	}
	
	public static final int ID_BYTES = 16;
	public static final int COMMAND_BYTES = 1;
	public static final int CODE_BYTES = 1;
	public static final int KEY_BYTES = 32;
	public static final int VALUE_LENGTH_BYTES = 2;
	public static final int VALUE_BYTES = 15000;
	public static final int LIFETIME = 5000;
	
	public static final int MIN_BYTES = ID_BYTES + COMMAND_BYTES;
	public static final int REQ_MAX_BYTES = MIN_BYTES + KEY_BYTES + VALUE_LENGTH_BYTES + VALUE_BYTES;
	public static final int RES_MAX_BYTES = MIN_BYTES + VALUE_LENGTH_BYTES + VALUE_BYTES;
	
	private Message() {}
	
	public static byte[] id(byte[] msg) {
		return Arrays.copyOfRange(msg, 0, ID_BYTES);
	}
	
	public static boolean sameId(byte[] msgA, byte[] msgB) {
		if (msgA.length < ID_BYTES || msgB.length < ID_BYTES) {
			return false;
		}
		
		for (int i = 0; i < ID_BYTES; i++) {
			if (msgA[i] != msgB[i]) {
				return false;
			}
		}
		
		return true;
	}

	public static Command command(byte[] request) {
		switch (request[ID_BYTES]) {
			case 0x01: return Command.PUT;
			case 0x02: return Command.GET;
			case 0x03: return Command.REMOVE;
			case 0x04: return Command.SHUTDOWN;
			case 0x05: return Command.HEARTBEAT_REQ;
			default: return Command.UNKNOWN;
		}
	}

	public static byte command(Command command) {
		switch (command) {
			case PUT: return 0x01;
			case GET: return 0x02;
			case REMOVE: return 0x03;
			case SHUTDOWN: return 0x04;
			case HEARTBEAT_REQ: return 0x05;
			default: return 0x06;
		}
	}
	
	public static Code code(byte[] response) {
		switch (response[ID_BYTES]) {
			case 0x00: return Code.SUCCESSFUL;
			case 0x01: return Code.NON_EXISTENT_KEY;
			case 0x02: return Code.OUT_OF_SPACE;
			case 0x03: return Code.SYSTEM_OVERLOAD;
			case 0x04: return Code.INTERNAL_FAILURE;
			case 0x06: return Code.HEARTBEAT_ACK;
			default: return Code.UNRECOGNIZED_COMMAND;
		}
	}
	
	private static byte code(Code code) {
		switch (code) {
			case SUCCESSFUL: return 0x00;
			case NON_EXISTENT_KEY: return 0x01;
			case OUT_OF_SPACE: return 0x02;
			case SYSTEM_OVERLOAD: return 0x03;
			case INTERNAL_FAILURE: return 0x04;
			case HEARTBEAT_ACK: return 0x06;
			default: return 0x05;
		}
	}

	public static byte[] key(byte[] request) {
		return Arrays.copyOfRange(request, MIN_BYTES, MIN_BYTES + KEY_BYTES);
	}
	
	public static byte[] requestValue(byte[] request) {
		if (request.length <= Message.MIN_BYTES) {
			return new byte[0];
		}
		
		int firstByteIndex = Message.MIN_BYTES + Message.KEY_BYTES;
		int secondByteIndex = firstByteIndex + 1;
		int length = (request[firstByteIndex] & 0xFF) + ((request[secondByteIndex] << 8) & 0xFF00);
		
		int start = Message.MIN_BYTES + Message.KEY_BYTES + Message.VALUE_LENGTH_BYTES;
		int end = start + length;
		return Arrays.copyOfRange(request, start, end);
	}
	
	public static byte[] createRequest(Command command) {
		byte[] req = new byte[MIN_BYTES];
		Random r = new Random();
		r.nextBytes(req);
		req[req.length - 1] = command(command);
		return req;
	}
	
	public static byte[] createResponse(byte[] id, Code code) {
		byte[] result = new byte[MIN_BYTES];
		System.arraycopy(id, 0, result, 0, Math.min(id.length, ID_BYTES));
		result[ID_BYTES] = code(code);
		return result;
	}

	public static byte[] createResponse(byte[] id, Code code, byte[] value) {
		int valueLength = Math.min(value.length, VALUE_BYTES);
		byte[] result = new byte[ID_BYTES + CODE_BYTES + VALUE_LENGTH_BYTES + valueLength];
		System.arraycopy(id, 0, result, 0, Math.min(id.length, ID_BYTES));
		result[ID_BYTES] = code(code);
		result[ID_BYTES + CODE_BYTES] = (byte) (valueLength & 0xFF);
		result[ID_BYTES + CODE_BYTES + 1] = (byte) ((valueLength >> 8) & 0xFF);
		System.arraycopy(value, 0, result, ID_BYTES + CODE_BYTES + VALUE_LENGTH_BYTES, valueLength);
		return result;
	}
}
