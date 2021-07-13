/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.conf;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.hops.codegen.SpoofCompiler.CompilerType;
import org.apache.sysds.hops.codegen.SpoofCompiler.GeneratorAPI;
import org.apache.sysds.hops.codegen.SpoofCompiler.PlanSelector;
import org.apache.sysds.lops.Compression;
import org.apache.sysds.parser.ParseException;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class DMLConfig
{
	public static final String DEFAULT_SYSTEMDS_CONFIG_FILEPATH = "./SystemDS-config.xml";
	
	private static final Log LOG = LogFactory.getLog(DMLConfig.class.getName());
	
	// external names of configuration properties 
	// (single point of change for all internal refs)
	public static final String LOCAL_TMP_DIR        = "sysds.localtmpdir";
	public static final String SCRATCH_SPACE        = "sysds.scratch";
	public static final String OPTIMIZATION_LEVEL   = "sysds.optlevel";
	public static final String DEFAULT_BLOCK_SIZE   = "sysds.defaultblocksize";
	public static final String CP_PARALLEL_OPS      = "sysds.cp.parallel.ops";
	public static final String CP_PARALLEL_IO       = "sysds.cp.parallel.io";
	public static final String COMPRESSED_LINALG    = "sysds.compressed.linalg";
	public static final String COMPRESSED_LOSSY     = "sysds.compressed.lossy";
	public static final String COMPRESSED_VALID_COMPRESSIONS = "sysds.compressed.valid.compressions";
	public static final String COMPRESSED_OVERLAPPING = "sysds.compressed.overlapping"; 
	public static final String COMPRESSED_SAMPLING_RATIO = "sysds.compressed.sampling.ratio"; 
	public static final String COMPRESSED_COCODE    = "sysds.compressed.cocode"; 
	public static final String COMPRESSED_COST_MODEL= "sysds.compressed.costmodel";
	public static final String COMPRESSED_TRANSPOSE = "sysds.compressed.transpose";
	public static final String NATIVE_BLAS          = "sysds.native.blas";
	public static final String NATIVE_BLAS_DIR      = "sysds.native.blas.directory";
	public static final String CODEGEN              = "sysds.codegen.enabled"; //boolean
	public static final String CODEGEN_API          = "sysds.codegen.api"; // see SpoofCompiler.API
	public static final String CODEGEN_COMPILER     = "sysds.codegen.compiler"; //see SpoofCompiler.CompilerType
	public static final String CODEGEN_OPTIMIZER    = "sysds.codegen.optimizer"; //see SpoofCompiler.PlanSelector
	public static final String CODEGEN_PLANCACHE    = "sysds.codegen.plancache"; //boolean
	public static final String CODEGEN_LITERALS     = "sysds.codegen.literals"; //1..heuristic, 2..always
	public static final String STATS_MAX_WRAP_LEN   = "sysds.stats.maxWrapLength"; //int
	public static final String AVAILABLE_GPUS       = "sysds.gpu.availableGPUs"; // String to specify which GPUs to use (a range, all GPUs, comma separated list or a specific GPU)
	public static final String SYNCHRONIZE_GPU      = "sysds.gpu.sync.postProcess"; // boolean: whether to synchronize GPUs after every instruction
	public static final String EAGER_CUDA_FREE      = "sysds.gpu.eager.cudaFree"; // boolean: whether to perform eager CUDA free on rmvar
	public static final String GPU_EVICTION_POLICY  = "sysds.gpu.eviction.policy"; // string: can be lru, lfu, min_evict
	public static final String LOCAL_SPARK_NUM_THREADS = "sysds.local.spark.number.threads";
	public static final String CACHE                = "sysds.cache"; // true false
	public static final String LINEAGECACHESPILL    = "sysds.lineage.cachespill"; // boolean: whether to spill cache entries to disk
	public static final String COMPILERASSISTED_RW  = "sysds.lineage.compilerassisted"; // boolean: whether to apply compiler assisted rewrites
	
	// Fraction of available memory to use. The available memory is computer when the GPUContext is created
	// to handle the tradeoff on calling cudaMemGetInfo too often.
	public static final String GPU_MEMORY_UTILIZATION_FACTOR = "sysds.gpu.memory.util.factor";
	public static final String GPU_MEMORY_ALLOCATOR = "sysds.gpu.memory.allocator"; // String to specify the memory allocator to use. Supported values are: cuda, unified_memory
	public static final String FLOATING_POINT_PRECISION = "sysds.floating.point.precision"; // String to specify the datatype to use internally: supported values are double, single
	public static final String PRINT_GPU_MEMORY_INFO = "sysds.gpu.print.memoryInfo";
	public static final String EVICTION_SHADOW_BUFFERSIZE = "sysds.gpu.eviction.shadow.bufferSize";

	public static final String USE_SSL_FEDERATED_COMMUNICATION = "sysds.federated.ssl"; // boolean
	public static final String DEFAULT_FEDERATED_INITIALIZATION_TIMEOUT = "sysds.federated.initialization.timeout"; // int seconds
	public static final int DEFAULT_FEDERATED_PORT = 4040; // borrowed default Spark Port
	public static final int DEFAULT_NUMBER_OF_FEDERATED_WORKER_THREADS = 2;
	
	//internal config
	public static final String DEFAULT_SHARED_DIR_PERMISSION = "777"; //for local fs and DFS
	
	//configuration default values
	private static HashMap<String, String> _defaultVals = null;

	private String _fileName = null;
	private Element _xmlRoot = null;
	private DocumentBuilder _documentBuilder = null;
	private Document _document = null;
	
	static
	{
		_defaultVals = new HashMap<>();
		_defaultVals.put(LOCAL_TMP_DIR,          "/tmp/systemds" );
		_defaultVals.put(SCRATCH_SPACE,          "scratch_space" );
		_defaultVals.put(OPTIMIZATION_LEVEL,     String.valueOf(OptimizerUtils.DEFAULT_OPTLEVEL.ordinal()) );
		_defaultVals.put(DEFAULT_BLOCK_SIZE,     String.valueOf(OptimizerUtils.DEFAULT_BLOCKSIZE) );
		_defaultVals.put(CP_PARALLEL_OPS,        "true" );
		_defaultVals.put(CP_PARALLEL_IO,         "true" );
		_defaultVals.put(COMPRESSED_LINALG,      Compression.CompressConfig.FALSE.name() );
		_defaultVals.put(COMPRESSED_LOSSY,       "false" );
		_defaultVals.put(COMPRESSED_VALID_COMPRESSIONS, "SDC,DDC");
		_defaultVals.put(COMPRESSED_OVERLAPPING, "true" );
		_defaultVals.put(COMPRESSED_SAMPLING_RATIO, "0.01");
		_defaultVals.put(COMPRESSED_COCODE,      "AUTO");
		_defaultVals.put(COMPRESSED_COST_MODEL,  "AUTO");
		_defaultVals.put(COMPRESSED_TRANSPOSE,   "auto");
		_defaultVals.put(CODEGEN,                "false" );
		_defaultVals.put(CODEGEN_API,            GeneratorAPI.JAVA.name() );
		_defaultVals.put(CODEGEN_COMPILER,       CompilerType.AUTO.name() );
		_defaultVals.put(CODEGEN_OPTIMIZER,      PlanSelector.FUSE_COST_BASED_V2.name() );
		_defaultVals.put(CODEGEN_PLANCACHE,      "true" );
		_defaultVals.put(CODEGEN_LITERALS,       "1" );
		_defaultVals.put(NATIVE_BLAS,            "none" );
		_defaultVals.put(NATIVE_BLAS_DIR,        "none" );
		_defaultVals.put(CACHE,                  "true" );
		_defaultVals.put(LINEAGECACHESPILL,      "true" );
		_defaultVals.put(COMPILERASSISTED_RW,    "true" );
		_defaultVals.put(PRINT_GPU_MEMORY_INFO,  "false" );
		_defaultVals.put(EVICTION_SHADOW_BUFFERSIZE,  "0.0" );
		_defaultVals.put(STATS_MAX_WRAP_LEN,     "30" );
		_defaultVals.put(GPU_MEMORY_UTILIZATION_FACTOR,      "0.9" );
		_defaultVals.put(GPU_MEMORY_ALLOCATOR,   "cuda");
		_defaultVals.put(AVAILABLE_GPUS,         "-1");
		_defaultVals.put(GPU_EVICTION_POLICY,    "min_evict");
		_defaultVals.put(LOCAL_SPARK_NUM_THREADS, "*"); // * Means it allocates the number of available threads on the local host machine.
		_defaultVals.put(SYNCHRONIZE_GPU,        "false" );
		_defaultVals.put(EAGER_CUDA_FREE,        "false" );
		_defaultVals.put(FLOATING_POINT_PRECISION, "double" );
		_defaultVals.put(USE_SSL_FEDERATED_COMMUNICATION, "false");
		_defaultVals.put(DEFAULT_FEDERATED_INITIALIZATION_TIMEOUT, "10");
	}
	
	public DMLConfig() {
		
	}

	public DMLConfig(String fileName) 
		throws FileNotFoundException
	{
		this( fileName, false );
	}
	
	public DMLConfig(String fileName, boolean silent) 
		throws FileNotFoundException
	{
		_fileName = fileName;
		try {
			parseConfig();
		} catch (FileNotFoundException fnfe) {
			throw fnfe;
		} catch (Exception e){
			//log error, since signature of generated ParseException doesn't allow to pass it 
			if( !silent )
				LOG.error("Failed to parse DML config file ",e);
			throw new ParseException("ERROR: error parsing DMLConfig file " + fileName);
		}
	}
	
	public DMLConfig( Element root ) {
		_xmlRoot = root;
	}
	
	public DMLConfig( DMLConfig dmlconf ) {
		set(dmlconf);
	}
	
	public void set(DMLConfig dmlconf) {
		_fileName = dmlconf._fileName;
		_xmlRoot = dmlconf._xmlRoot;
		_documentBuilder = dmlconf._documentBuilder;
		_document = dmlconf._document;
	}

	/**
	 * Method to parse configuration
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	private void parseConfig () throws ParserConfigurationException, SAXException, IOException 
	{
		DocumentBuilder builder = getDocumentBuilder();
		_document = null;
		if( _fileName.startsWith("hdfs:") || _fileName.startsWith("gpfs:")
			|| IOUtilFunctions.isObjectStoreFileScheme(new Path(_fileName)) )
		{
			Path configFilePath = new Path(_fileName);
			try( FileSystem DFS = IOUtilFunctions.getFileSystem(configFilePath) ) {
				_document = builder.parse(DFS.open(configFilePath));
			}
		}
		else  // config from local file system
		{
			_document = builder.parse(_fileName);
		}

		_xmlRoot = _document.getDocumentElement();
	}

	private DocumentBuilder getDocumentBuilder() throws ParserConfigurationException {
		if (_documentBuilder == null) {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setIgnoringComments(true); //ignore XML comments
			_documentBuilder = factory.newDocumentBuilder();
		}
		return _documentBuilder;
	}

	/**
	 * Method to get string value of a configuration parameter
	 * Handles processing of configuration parameters 
	 * @param tagName the name of the DMLConfig parameter being retrieved
	 * @return a string representation of the DMLConfig parameter value.  
	 */
	public String getTextValue(String tagName) 
	{
		//get the actual value
		String retVal = (_xmlRoot!=null)?getTextValue(_xmlRoot,tagName):null;
		
		if (retVal == null)
		{
			if( _defaultVals.containsKey(tagName) )
				retVal = _defaultVals.get(tagName);
			else
				LOG.error("Error: requested dml configuration property '"+tagName+"' is invalid.");
		}
		
		return retVal;
	}
	
	public int getIntValue( String tagName )
	{
		return Integer.parseInt( getTextValue(tagName) );
	}
	
	public boolean getBooleanValue( String tagName )
	{
		return Boolean.parseBoolean( getTextValue(tagName) );
	}
	
	public double getDoubleValue( String tagName )
	{
		return Double.parseDouble( getTextValue(tagName) );
	}
	
	/**
	 * Method to get the string value of an element identified by a tag name
	 * @param element the DOM element
	 * @param tagName the tag name
	 * @return the string value of the element
	 */
	private static String getTextValue(Element element, String tagName) {
		String textVal = null;
		NodeList list = element.getElementsByTagName(tagName);
		if (list != null && list.getLength() > 0) {
			Element elem = (Element) list.item(0);
			textVal = elem.getFirstChild().getNodeValue();
			
		}
		return textVal;
	}
	

	/**
	 * Method to update the key value
	 * @param paramName parameter name
	 * @param paramValue parameter value
	 */
	public void setTextValue(String paramName, String paramValue) {
		if(_xmlRoot != null) {
			NodeList list = _xmlRoot.getElementsByTagName(paramName);
			if (list != null && list.getLength() > 0) {
				Element elem = (Element) list.item(0);
				elem.getFirstChild().setNodeValue(paramValue);
			} else {
				Node value = _document.createTextNode(paramValue);
				Node element = _document.createElement(paramName);
				element.appendChild(value);
				_xmlRoot.appendChild(element);
			}
		} else {
			try {
				DocumentBuilder builder = getDocumentBuilder();
				String configString = "<root><" + paramName + ">"+paramValue+"</" + paramName + "></root>";
				_document = builder.parse(new ByteArrayInputStream(configString.getBytes("UTF-8")));
				_xmlRoot = _document.getDocumentElement();
			} catch (Exception e) {
				throw new DMLRuntimeException("Unable to set config value", e);
			}
		}
	}

	public synchronized String serializeDMLConfig() 
	{
		String ret = null;
		try {
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			//transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			StreamResult result = new StreamResult(new StringWriter());
			DOMSource source = new DOMSource(_xmlRoot);
			transformer.transform(source, result);
			ret = result.getWriter().toString();
		}
		catch(Exception ex) {
			throw new DMLRuntimeException("Unable to serialize DML config.", ex);
		}
		
		return ret;
	}
	
	public static DMLConfig parseDMLConfig( String content ) {
		DMLConfig ret = null;
		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document domTree = null;
			domTree = builder.parse( new ByteArrayInputStream(content.getBytes("utf-8")) );
			Element root = domTree.getDocumentElement();
			ret = new DMLConfig( root );
		}
		catch(Exception ex) {
			throw new DMLRuntimeException("Unable to parse DML config.", ex);
		}
		
		return ret;
	}
	
	/**
	 * Start with the internal default settings, then merge in the
	 * settings from any specified configuration file, if available.
	 * If it is not explicitly given, then merge in settings from
	 * the default configuration file location, if available.
	 *
	 * @param configPath User-defined path of the configuration file.
	 * @return dml configuration
	 * @throws FileNotFoundException if FileNotFoundException occurs
	 */
	public static DMLConfig readConfigurationFile(String configPath)
		throws FileNotFoundException
	{
		// Always start with the internal defaults
		DMLConfig config = new DMLConfig();

		// Merge in any specified or default configs if available
		if (configPath != null) {
			// specified
			try {
				config = new DMLConfig(configPath, false);
			} catch (FileNotFoundException fnfe) {
				LOG.error("Custom config file " + configPath + " not found.");
				throw fnfe;
			} catch (ParseException e) {
				throw e;
			}
		} else {
			// default
			try {
				config = new DMLConfig(DEFAULT_SYSTEMDS_CONFIG_FILEPATH, false);
			} catch (FileNotFoundException fnfe) {
				LOG.info("Using internal default configuration settings.  If you wish to " +
						 "customize any settings, please supply a `SystemDS-config.xml` file.");
				config = new DMLConfig();
			} catch (ParseException e) {
				throw e;
			}
		}
		return config;
	}

	public String getConfigInfo()  {
		String[] tmpConfig = new String[] { 
			LOCAL_TMP_DIR,SCRATCH_SPACE,OPTIMIZATION_LEVEL, DEFAULT_BLOCK_SIZE,
			CP_PARALLEL_OPS, CP_PARALLEL_IO, NATIVE_BLAS, NATIVE_BLAS_DIR,
			COMPRESSED_LINALG, COMPRESSED_LOSSY, COMPRESSED_VALID_COMPRESSIONS, COMPRESSED_OVERLAPPING,
			COMPRESSED_SAMPLING_RATIO, COMPRESSED_COCODE, COMPRESSED_TRANSPOSE,
			CODEGEN, CODEGEN_API, CODEGEN_COMPILER, CODEGEN_OPTIMIZER, CODEGEN_PLANCACHE, CODEGEN_LITERALS,
			STATS_MAX_WRAP_LEN, LINEAGECACHESPILL, COMPILERASSISTED_RW, PRINT_GPU_MEMORY_INFO,
			AVAILABLE_GPUS, SYNCHRONIZE_GPU, EAGER_CUDA_FREE, FLOATING_POINT_PRECISION, GPU_EVICTION_POLICY, 
			LOCAL_SPARK_NUM_THREADS, EVICTION_SHADOW_BUFFERSIZE, GPU_MEMORY_ALLOCATOR, GPU_MEMORY_UTILIZATION_FACTOR,
			USE_SSL_FEDERATED_COMMUNICATION, DEFAULT_FEDERATED_INITIALIZATION_TIMEOUT
		}; 
		
		StringBuilder sb = new StringBuilder();
		for( String tmp : tmpConfig ) {
			sb.append("INFO: ");
			sb.append(tmp);
			sb.append(": ");
			sb.append(getTextValue(tmp));
			sb.append("\n");
		}
		
		return sb.toString();
	}
	
	public static String getDefaultTextValue( String key ) {
		return _defaultVals.get( key );
	}
	
	@Override
	public DMLConfig clone() {
		DMLConfig conf = new DMLConfig();
		conf._fileName = _fileName;
		conf._xmlRoot = (Element) _xmlRoot.cloneNode(true);
		
		return conf;
	}
}
