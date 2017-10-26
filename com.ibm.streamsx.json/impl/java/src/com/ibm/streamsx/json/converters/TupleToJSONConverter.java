package com.ibm.streamsx.json.converters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.MapType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;
//import com.ibm.streams.operator.encoding.JSONTupleEncoding;

/**
 * Converts SPL tuples and SPL tuple attributes to String representations of JSON values.  
 */
@SuppressWarnings("rawtypes")
public class TupleToJSONConverter { 
	static final JsonFactory jfactory = new JsonFactory();
	static int outputStreamSize = 512;

	/**
	 * Converts an SPL tuple to a String representation of a JSONObject.
	 * @param tuple
	 * @return
	 * @throws IOException
	 */
	public static String convertTuple(Tuple tuple) throws IOException  {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(outputStreamSize);
		JsonGenerator jGenerator = jfactory.createGenerator(outputStream);
		jGenerator.writeStartObject();
		writeTupleToOut(jGenerator, tuple);
		jGenerator.writeEndObject();
		jGenerator.close();
		// outputStreamSize = outputStream.size();
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
		final JsonGenerator jGenerator = jfactory.createGenerator(outputStream);
		final Attribute attr = tuple.getStreamSchema().getAttribute(attrName);
		jGenerator.writeStartArray();
		writeArrayToOut(jGenerator, (CollectionType) attr.getType(), (Collection) tuple.getList(attr.getIndex()));
		jGenerator.writeEndArray();
		jGenerator.close();
		return outputStream.toString();
	}
	
	/** Writes tuple to jGenerator's out stream.
	 * @param jGenerator
	 * @param tuple
	 * @throws IOException
	 */
	public static void writeTupleToOut(JsonGenerator jGenerator, Tuple tuple) throws IOException {
        // for (Attribute attr : tuple.getStreamSchema())
        // {	
        //     processAttributeObject(jGenerator, tuple, attr);
        //     Type attrType = attr.getType();
        //     jsonObjectFromElement(jGenerator, attrType, tuple)
        // }

        StreamSchema schema = tuple.getStreamSchema();
        for (int i=0; i<schema.getAttributeCount(); i++) {
            Attribute attr = schema.getAttribute(i);
            //Type attrType = schema.getAttribute(i).getType();
            // jsonObjectFromElement(jGenerator, attrType, tuple.getObject(i, attrType));
            // dont actually need to cast here
            jGenerator.writeFieldName(attr.getName());
            jsonObjectFromElement(jGenerator, attr.getType(), tuple.getObject(i));

        }
	}
	
	/** Writes map to jGenerator's out stream.
	 * @param jGenerator
	 * @param type
	 * @param map
	 * @throws IOException
	 */
	public static void writeMapToOut(JsonGenerator jGenerator, MapType type, Map map) throws IOException {
        Type valueType = type.getValueType();
        Type keyType = type.getKeyType();
        switch (keyType.getMetaType()) {
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
                throw unsupported(type);
        }
//        for (Object k : map.keySet()) {
//            jGenerator.writeFieldName(k.toString());
//            // jsonObjectFromElement(jGenerator, typeInMap, k.toString(), map.get(k));
//            jsonObjectFromElement(jGenerator, valueType, map.get(k));
//        }
        
        // iterator version
        Set<Map.Entry> entrySet = map.entrySet();
        for (Map.Entry entry : entrySet) {
        	jGenerator.writeFieldName(entry.getKey().toString());
        	jsonObjectFromElement(jGenerator, valueType, entry.getValue());
        }
			
	}
	
	
	/**
	 * Writes array to jGenerator's out stream.
	 * @param jGenerator
	 * @param type
	 * @param c
	 * @throws IOException
	 */
	public static void writeArrayToOut(JsonGenerator jGenerator, CollectionType type, final Collection c) throws IOException {
		for (Object e : c) {
			jsonObjectFromElement(jGenerator, type.getElementType(), e);
		}
	}
	
	
	
//    /**
//     * Writes objects to jGenerator's out stream.
//     * @param jGenerator
//     * @param tuple
//     * @param name
//     * @throws IOException
//     */
//    public static void processAttributeObject(JsonGenerator jGenerator, final Tuple tuple, final String name) throws IOException {
//        processAttributeObject(jGenerator, tuple, tuple.getStreamSchema().getAttribute(name));
//    }
//    
//    
//    public static void processAttributeObject(JsonGenerator jGenerator, final Tuple tuple, final Attribute attr) throws IOException {
//    	//Object value;
//    	//final int index = attr.getIndex();
//    	final String attrName = attr.getName();
//    	//value = tuple.getObject(attrName);
//    	switch (attr.getType().getMetaType()) {
//    	case BOOLEAN:
//    		jGenerator.writeBooleanField(attrName, tuple.getBoolean(attrName));
//    		break;
//    	case INT8:
//    		jGenerator.writeNumberField(attrName, tuple.getByte(attrName));
//    		break;
//    	case INT16:
//    		jGenerator.writeNumberField(attrName, tuple.getShort(attrName));
//    		break;
//        case INT32:
//        	jGenerator.writeNumberField(attrName, tuple.getInt(attrName));
//        	break;
//        case INT64:
//        	jGenerator.writeNumberField(attrName, tuple.getLong(attrName));
//        	break;
//        case FLOAT32:
//        	jGenerator.writeNumberField(attrName, tuple.getFloat(attrName));
//        	break;
//        case FLOAT64:
//        	jGenerator.writeNumberField(attrName, tuple.getDouble(attrName));
//        	break;
//        case DECIMAL32:
//        case DECIMAL64:
//        case DECIMAL128:
//        	jGenerator.writeNumberField(attrName, tuple.getBigDecimal(attrName));
//        	break;
//        case USTRING:
//        case BSTRING:
//        case ENUM:
//            jGenerator.writeStringField(attrName, tuple.getString(attrName));
//            break;
//        case UINT8:
//        case UINT16:
//        	jGenerator.writeNumberField(attrName, tuple.getInt(attrName));
//        	break;
//        case UINT32:
//            // Get the unsigned value
//            jGenerator.writeNumberField(attrName, tuple.getLong(attrName));
//            break;
//        case UINT64:
//            // Get the unsigned value
//            jGenerator.writeNumberField(attrName, tuple.getBigDecimal(attrName));
//            break;
//        case RSTRING:
//        	jGenerator.writeStringField(attrName, tuple.getString(attrName));
//        	break;
//        case TUPLE:
//        	jGenerator.writeObjectFieldStart(attrName);
//        	writeTupleToOut(jGenerator, tuple.getTuple(attrName));
//        	jGenerator.writeEndObject();
//            break;
//        case TIMESTAMP:
//            jGenerator.writeNumberField(attrName, tuple.getTimestamp(attrName).getTimeAsSeconds());
//            break;
//            
//        case BMAP:
//        case MAP:
//        {
//            MapType mt = (MapType) attr.getType();
//            switch (mt.getKeyType().getMetaType()) {
//            // disallow key types that should not be Java
//            // keys since their toString() value is not a
//            // representation of the value
//            case XML:
//            case BLOB:
//            case TIMESTAMP:
//            case BLIST:
//            case LIST:
//            case MAP:
//            case BMAP:
//            case SET:
//            case BSET:
//            case COMPLEX32:
//            case COMPLEX64:
//            case TUPLE:
//                throw unsupported(mt);
//            }
//            jGenerator.writeObjectFieldStart(attrName);
//            writeMapToOut(jGenerator, mt, tuple.getMap(attrName));
//            jGenerator.writeEndObject();
//            break;
//        }
//        case SET:
//        case BSET:
//        {
//        	jGenerator.writeArrayFieldStart(attrName);
//        	writeArrayToOut(jGenerator, (CollectionType) attr.getType(), (Collection) tuple.getSet(attrName));
//            jGenerator.writeEndArray();
//            break;
//        }
//        case LIST:
//        case BLIST:
//        {
//        	jGenerator.writeArrayFieldStart(attrName);
//        	writeArrayToOut(jGenerator, (CollectionType) attr.getType(), (Collection) tuple.getList(attrName));
//            jGenerator.writeEndArray();
//            break;
//        }
//        case XML:
//        case BLOB:
//		default:
//			throw unsupported(attr.getType());
//    		
//    	}
//    	
//    }
    
    // private static void jsonObjectFromElement(JsonGenerator jGenerator, Type elementType, String attrName, Object cv) throws IOException {
    //     switch (elementType.getMetaType()) {
    //     case XML:
    //         throw unsupported(elementType);
    //     case RSTRING:
    //     case BSTRING:
    //     	jGenerator.writeStringField(attrName, cv.toString());
    //         break;
    //     case MAP:
    //     case BMAP:
    //     	jGenerator.writeObjectFieldStart(attrName);
    //     	writeMapToOut(jGenerator, (MapType) elementType, (Map) cv);
    //     	jGenerator.writeEndObject();
    //         break;
    //     case SET:
    //     case BSET:
    //     case LIST:
    //     case BLIST:
    //     	jGenerator.writeArrayFieldStart(attrName);
    //     	writeArrayToOut(jGenerator, (CollectionType) elementType, (Collection) cv);
    //         jGenerator.writeEndArray();
    //         break;
    //     case TIMESTAMP:
    //     	jGenerator.writeNumberField(attrName, ((Timestamp) cv).getTimeAsSeconds());
    //     	break;
    //     case TUPLE:
    //     	jGenerator.writeObjectFieldStart(attrName);
    //     	writeTupleToOut(jGenerator, (Tuple) cv);
    //     	jGenerator.writeEndObject();
    //         break;
    //     case UINT8:
    //     	jGenerator.writeNumberField(attrName, (((Byte) cv).intValue()) & 0xFF);
    //     	break;
    //     case UINT16:
    //     	jGenerator.writeNumberField(attrName, (((Short) cv).intValue()) & 0xFFFF);
    //     	break;
    //     case UINT32:
    //     	jGenerator.writeNumberField(attrName, (((Integer) cv).longValue()) & 0xFFFFFFFFL);
    //     	break;
    //     case UINT64:
    //     	jGenerator.writeNumberField(attrName, new BigDecimal((Long) cv));
    //         break;
    //     default:
    //     	jGenerator.writeObjectField(attrName, cv);
    //     	break;
    //     }
    // }
    
    private static void jsonObjectFromElement(JsonGenerator jGenerator, Type elementType, Object cv) throws IOException {
        switch (elementType.getMetaType()) {
        case XML:
            throw unsupported(elementType);
        case RSTRING:
        case BSTRING:
        	jGenerator.writeString(cv.toString());
            break;
        case MAP:
        case BMAP:
        	jGenerator.writeStartObject();
        	writeMapToOut(jGenerator, (MapType) elementType, (Map) cv);
        	jGenerator.writeEndObject();
            break;
        case SET:
        case BSET:
        case LIST:
        case BLIST:
        	jGenerator.writeStartArray();
        	writeArrayToOut(jGenerator, (CollectionType) elementType, (Collection) cv);
            jGenerator.writeEndArray();
            break;
        case TIMESTAMP:
        	jGenerator.writeNumber(((Timestamp) cv).getTimeAsSeconds());
        	break;
        case TUPLE:
        	jGenerator.writeStartObject();
        	writeTupleToOut(jGenerator, (Tuple) cv);
        	jGenerator.writeEndObject();
            break;
        case UINT8:
        	jGenerator.writeNumber((((Byte) cv).intValue()) & 0xFF);
        	break;
        case UINT16:
        	jGenerator.writeNumber((((Short) cv).intValue()) & 0xFFFF);
        	break;
        case UINT32:
        	jGenerator.writeNumber((((Integer) cv).longValue()) & 0xFFFFFFFFL);
        	break;
        case UINT64:
        	jGenerator.writeNumber(new BigDecimal((Long) cv));
            break;
        default:
        	jGenerator.writeObject(cv);
        }
    }
    
    private static UnsupportedOperationException unsupported(Type type) {
        return new UnsupportedOperationException(
                MessageFormat.format("JSON encoding not supported for {1} (MetaType.{0}))",
                        type.getMetaType(), type.getLanguageType()));
    }
}
