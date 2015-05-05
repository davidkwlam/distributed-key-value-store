package com.davidkwlam.kvstore;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.bind.DatatypeConverter;

public class Utils {

	private Utils() {}
	
	public static String hexString(byte[] bytes) {
		return DatatypeConverter.printHexBinary(bytes);
	}

	public static void print(Object o) {
		System.out.println(o);
	}
	
	public static ArrayList<String> readNodesFile(String filename) throws IOException {
		ArrayList<String> nodes = new ArrayList<>();
		
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line;
		while ((line = br.readLine()) != null) {
			nodes.add(line.trim());
		}
		br.close();
		
		return nodes;
	}
	
}
