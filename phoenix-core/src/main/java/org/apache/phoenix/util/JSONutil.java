package org.apache.phoenix.util;

import java.io.IOException;
import java.util.Map;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONutil {
	ObjectMapper mapper=null;
	public JSONutil(){
		mapper=new ObjectMapper();
	}
	public static boolean isJSON(String value){
		ObjectMapper mapper = new ObjectMapper();
		try{
			mapper.readValue(value, Map.class);
			return true;
		}
		catch(JsonParseException e){
			return false;
		}
		catch(IOException e){
			return false;
		}
	}
	public static boolean isJSON(byte[] value){
		ObjectMapper mapper = new ObjectMapper();
		try{
			mapper.readValue(value, Map.class);
			return true;
		}
		catch(JsonParseException e){
			return false;
		}
		catch(IOException e){
			return false;
		}
	}
	public Object mapJSON(Object index,Object json) throws JsonParseException, JsonMappingException, IOException
	{
		Map<String, Object> map = mapper.readValue((String)json, Map.class);
		Object o= map.get(index);
		return o;
	}
	public Map<String, Object> getStringMap(Object json) throws JsonParseException, JsonMappingException, IOException{
		return mapper.readValue((String)json, Map.class);
	}
	public JsonNode getJsonNode(Object json) throws JsonProcessingException, IOException{
		return mapper.readTree((String)json);
	}
	public JsonNode enterJsonTreeNode(JsonNode node,String nodename) throws JsonProcessingException, IOException{
		return node.path(nodename);  
	}
	public JsonNode enterJsonNodeArray(JsonNode node,int index) throws JsonProcessingException, IOException{
		return node.path(index);  
	}
}
