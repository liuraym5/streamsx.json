package com.ibm.streamsx.json.converters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Map;

import com.google.gson.stream.JsonWriter;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.MapType;
import com.ibm.streams.operator.types.Timestamp;

@SuppressWarnings("rawtypes")
public class TupleToJSONConverter_GSON {
	static int outputStreamSize = 512;

	/**
	 * Converts an SPL tuple to a String representation of a JSONObject.
	 * @param tuple
	 * @return
	 * @throws IOException
	 */
	public static String convertTuple(Tuple tuple) throws IOException  {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(outputStreamSize);
		JsonWriter writer = new JsonWriter(new OutputStreamWriter(outputStream, "UTF-8"));
		writer.beginObject();
		writeTupleToOut(writer, tuple);
		writer.endObject();
		writer.close();
		return outputStream.toString("UTF-8");
	}
	
	/**
	 * Converts an SPL tuple attribute (that must be a list) to a String representation of a JSONArray
	 * @param tuple Tuple containing the attribute to be converted
	 * @param attrName Name of the attribute to convert
	 * @return String representation of a JSON array
	 * @throws IOException If there was a problem converting the SPL tuple attribute
	 */
	public static String convertArray(Tuple tuple, String attrName) throws IOException  {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		JsonWriter writer = new JsonWriter(new OutputStreamWriter(outputStream, "UTF-8"));
		writer.beginArray();
		writeTupleToOut(writer, tuple);
		writer.endArray();
		writer.close();
		return outputStream.toString("UTF-8");
	}
	
	/** Writes tuple to jGenerator's out stream.
	 * @param writer
	 * @param tuple
	 * @throws IOException
	 */
	public static void writeTupleToOut(JsonWriter writer, Tuple tuple) throws IOException {
        for (Attribute attr : tuple.getStreamSchema())
        {	
        	processAttributeObject(writer, tuple, attr);
        }
	}
	
	/** Writes map to jGenerator's out stream.
	 * @param writer
	 * @param type
	 * @param map
	 * @throws IOException
	 */
	public static void writeMapToOut(JsonWriter writer, MapType type, Map map) throws IOException {
		for (Object k : map.keySet()) {
			jsonObjectFromElement(writer, type.getValueType(), k.toString(), map.get(k));
		}
			
	}
	
	
	/**
	 * Writes array to jGenerator's out stream.
	 * @param writer
	 * @param type
	 * @param c
	 * @throws IOException
	 */
	public static void writeArrayToOut(JsonWriter writer, CollectionType type, final Collection c) throws IOException {
		for (Object e : c) {
			jsonObjectFromElement(writer, type.getElementType(), e);
		}
	}
	
    /**
     * Writes objects to jGenerator's out stream.
     * @param writer
     * @param tuple
     * @param name
     * @throws IOException
     */
    public static void processAttributeObject(JsonWriter writer, final Tuple tuple, final String name) throws IOException {
        processAttributeObject(writer, tuple, tuple.getStreamSchema().getAttribute(name));
    }
    
    public static void processAttributeObject(JsonWriter writer, final Tuple tuple, final Attribute attr) throws IOException {
    	//Object value;
    	//final int index = attr.getIndex();
    	final String attrName = attr.getName();
    	//value = tuple.getObject(attrName);
    	switch (attr.getType().getMetaType()) {
    	case BOOLEAN:
    		writer.name(attrName).value(tuple.getBoolean(attrName));
    		break;
    	case INT8:
    		writer.name(attrName).value(tuple.getByte(attrName));
    		break;
    	case INT16:
    		writer.name(attrName).value( tuple.getShort(attrName));
    		break;
        case INT32:
        	writer.name(attrName).value(tuple.getInt(attrName));
        	break;
        case INT64:
        	writer.name(attrName).value(tuple.getLong(attrName));
        	break;
        case FLOAT32:
        	writer.name(attrName).value(tuple.getFloat(attrName));
        	break;
        case FLOAT64:
        	writer.name(attrName).value(tuple.getDouble(attrName));
        	break;
        case DECIMAL32:
        case DECIMAL64:
        case DECIMAL128:
        	writer.name(attrName).value(tuple.getBigDecimal(attrName));
        	break;
        case USTRING:
        case BSTRING:
        case ENUM:
            writer.name(attrName).value(tuple.getString(attrName));
            break;
        case UINT8:
        case UINT16:
        	writer.name(attrName).value(tuple.getInt(attrName));
        	break;
        case UINT32:
            // Get the unsigned value
            writer.name(attrName).value(tuple.getLong(attrName));
            break;
        case UINT64:
            // Get the unsigned value
            writer.name(attrName).value(tuple.getBigDecimal(attrName));
            break;
        case RSTRING:
        	writer.name(attrName).value(tuple.getString(attrName));
        	break;
        case TUPLE:
        	writer.name(attrName).beginObject();
        	writeTupleToOut(writer, tuple.getTuple(attrName));
        	writer.endObject();
            break;
        case TIMESTAMP:
            writer.name(attrName).value(tuple.getTimestamp(attrName).getTimeAsSeconds());
            break;
            
        case BMAP:
        case MAP:
        {
            MapType mt = (MapType) attr.getType();
            switch (mt.getKeyType().getMetaType()) {
            // disallow key types that should not be Java
            // keys since their toString() value is not a
            // representation of the value
            case XML:
            case BLOB:
            case TIMESTAMP:
            case BLIST:
            case LIST:
            case MAP:
            case BMAP:
            case SET:
            case BSET:
            case COMPLEX32:
            case COMPLEX64:
            case TUPLE:
                throw unsupported(mt);
            }
            writer.name(attrName).beginObject();
            writeMapToOut(writer, mt, tuple.getMap(attrName));
            writer.endObject();
            break;
        }
        case SET:
        case BSET:
        {
        	writer.name(attrName).beginArray();
        	writeArrayToOut(writer, (CollectionType) attr.getType(), (Collection) tuple.getSet(attrName));
            writer.endArray();
            break;
        }
        case LIST:
        case BLIST:
        {
        	writer.name(attrName).beginArray();
        	writeArrayToOut(writer, (CollectionType) attr.getType(), (Collection) tuple.getList(attrName));
            writer.endArray();
            break;
        }
        case XML:
        case BLOB:
		default:
			throw unsupported(attr.getType());
    		
    	}
    	
    }
    
    private static void jsonObjectFromElement(JsonWriter writer, Type elementType, String attrName, Object cv) throws IOException {
        switch (elementType.getMetaType()) {
        case XML:
            throw unsupported(elementType);
        case RSTRING:
        case BSTRING:
        	writer.name(attrName).value(cv.toString());
            break;
        case MAP:
        case BMAP:
        	writer.name(attrName).beginObject();
        	writeMapToOut(writer, (MapType) elementType, (Map) cv);
        	writer.endObject();
            break;
        case SET:
        case BSET:
        case LIST:
        case BLIST:
        	writer.name(attrName).beginArray();
        	writeArrayToOut(writer, (CollectionType) elementType, (Collection) cv);
            writer.endArray();
            break;
        case TIMESTAMP:
        	writer.name(attrName).value(((Timestamp) cv).getTimeAsSeconds());
        	break;
        case TUPLE:
        	writer.name(attrName).beginObject();
        	writeTupleToOut(writer, (Tuple) cv);
        	writer.endObject();
            break;
        case UINT8:
        	writer.name(attrName).value((((Byte) cv).intValue()) & 0xFF);
        	break;
        case UINT16:
        	writer.name(attrName).value((((Short) cv).intValue()) & 0xFFFF);
        	break;
        case UINT32:
        	writer.name(attrName).value((((Integer) cv).longValue()) & 0xFFFFFFFFL);
        	break;
        case UINT64:
        	writer.name(attrName).value(new BigDecimal((Long) cv));
            break;
        default:
        	writer.name(attrName).value(cv.toString());
        	break;
        }
    }
    
    private static void jsonObjectFromElement(JsonWriter writer, Type elementType, Object cv) throws IOException {
        switch (elementType.getMetaType()) {
        case XML:
            throw unsupported(elementType);
        case RSTRING:
        case BSTRING:
        	writer.value(cv.toString());
            break;
        case MAP:
        case BMAP:
        	writer.beginObject();
        	writeMapToOut(writer, (MapType) elementType, (Map) cv);
        	writer.endObject();
            break;
        case SET:
        case BSET:
        case LIST:
        case BLIST:
        	writer.beginArray();
        	writeArrayToOut(writer, (CollectionType) elementType, (Collection) cv);
            writer.endArray();
            break;
        case TIMESTAMP:
        	writer.value(((Timestamp) cv).getTimeAsSeconds());
        	break;
        case TUPLE:
        	writer.beginObject();
        	writeTupleToOut(writer, (Tuple) cv);
        	writer.endObject();
            break;
        case UINT8:
        	writer.value((((Byte) cv).intValue()) & 0xFF);
        	break;
        case UINT16:
        	writer.value((((Short) cv).intValue()) & 0xFFFF);
        	break;
        case UINT32:
        	writer.value((((Integer) cv).longValue()) & 0xFFFFFFFFL);
        	break;
        case UINT64:
        	writer.value(new BigDecimal((Long) cv));
            break;
        default:
        	writer.value(cv.toString());
        }
    }
    
    private static UnsupportedOperationException unsupported(Type type) {
        return new UnsupportedOperationException(
                MessageFormat.format("JSON encoding not supported for {1} (MetaType.{0}))",
                        type.getMetaType(), type.getLanguageType()));
    }
}
