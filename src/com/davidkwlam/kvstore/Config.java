package com.davidkwlam.kvstore;

public class Config {

	public final static int PORT_COORDINATOR = 55555;
	
	public final static int PORT_STORE = 55556;

	public final static int KVSTORE_SIZE = 10000;
	
	public final static int REPLICATION_FACTOR = 3;
	
	public final static int WRITE_QUORUM = REPLICATION_FACTOR / 2 + 1;
	
	public final static int READ_QUORUM = REPLICATION_FACTOR - WRITE_QUORUM + 1;
	
	public final static String HASHING_ALGORITHM = "SHA-512";
	
	private Config() {}
	
}
