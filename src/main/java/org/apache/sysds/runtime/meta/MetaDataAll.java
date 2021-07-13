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

package org.apache.sysds.runtime.meta;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.EnumUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types;
import org.apache.sysds.parser.BooleanIdentifier;
import org.apache.sysds.parser.ConstIdentifier;
import org.apache.sysds.parser.DataExpression;
import org.apache.sysds.parser.DataIdentifier;
import org.apache.sysds.parser.DoubleIdentifier;
import org.apache.sysds.parser.Expression;
import org.apache.sysds.parser.LanguageException;
import org.apache.sysds.parser.ParseException;
import org.apache.sysds.parser.StringIdentifier;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.privacy.PrivacyConstraint;
import org.apache.sysds.runtime.privacy.propagation.PrivacyPropagator;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.utils.JSONHelper;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

public class MetaDataAll extends DataIdentifier {

	private JSONObject _metaObj;

	protected String _formatTypeString;
	protected String _fineGrainedPrivacy;
	protected String _schema;
	protected String _delim = DataExpression.DEFAULT_DELIM_DELIMITER;
	protected boolean _hasHeader = false;
	protected boolean _sparseDelim = DataExpression.DEFAULT_DELIM_SPARSE;

	public MetaDataAll() {
		// do nothing
	}

	public MetaDataAll(String meta) {
		try {
			_metaObj = new JSONObject(meta);
		}
		catch(JSONException e) {
			e.printStackTrace();
		}
		setPrivacy(PrivacyConstraint.PrivacyLevel.None);
		parseMetaDataParams();
	}

	public MetaDataAll(BufferedReader br) {
		try {
			_metaObj = JSONHelper.parse(br);
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		setPrivacy(PrivacyConstraint.PrivacyLevel.None);
		parseMetaDataParams();
	}

	public MetaDataAll(String mtdFileName, boolean conditional, boolean parseMeta) {
		setFilename(mtdFileName);
		_metaObj = readMetadataFile(mtdFileName, conditional);
		setPrivacy(PrivacyConstraint.PrivacyLevel.None);
		if(parseMeta)
			parseMetaDataParams();
	}

	public JSONObject readMetadataFile(String filename, boolean conditional)
	{
		JSONObject retVal = new JSONObject();
		boolean exists = HDFSTool.existsFileOnHDFS(filename);
		boolean isDir = HDFSTool.isDirectory(filename);

		// CASE: filename is a directory -- process as a directory
		if( exists && isDir )
		{
			for(FileStatus stat : HDFSTool.getDirectoryListing(filename)) {
				Path childPath = stat.getPath(); // gives directory name
				if( !childPath.getName().startsWith("part") )
					continue;
				try (BufferedReader br = new BufferedReader(new InputStreamReader(
					IOUtilFunctions.getFileSystem(childPath).open(childPath))))
				{
					JSONObject childObj = JSONHelper.parse(br);
					for( Object obj : childObj.entrySet() ){
						@SuppressWarnings("unchecked") Map.Entry<Object,Object> e = (Map.Entry<Object, Object>) obj;
						Object key = e.getKey();
						Object val = e.getValue();
						retVal.put(key, val);
					}
				}
				catch( IOException e){
					raiseValidateError("for MTD file in directory, error parting part of MTD file with path " + childPath.toString() + ": " + e.getMessage(), conditional);
				}
			}
		}

		// CASE: filename points to a file
		else if (exists) {
			Path path = new Path(filename);
			try (BufferedReader br = new BufferedReader(new InputStreamReader(
				IOUtilFunctions.getFileSystem(path).open(path))))
			{
				retVal = new JSONObject(br);
			}
			catch (Exception e){
				raiseValidateError("error parsing MTD file with path " + filename + ": " + e.getMessage(), conditional);
			}
		}

		return retVal;
	}

	@SuppressWarnings("unchecked")
	private void parseMetaDataParams()
	{
		for( Object obj : _metaObj.entrySet() ){
			Map.Entry<Object,Object> e = (Map.Entry<Object, Object>) obj;
			Object key = e.getKey();
			Object val = e.getValue();

			boolean isValidName = DataExpression.READ_VALID_MTD_PARAM_NAMES.contains(key);

			if (!isValidName){ //wrong parameters always rejected
				raiseValidateError("MTD file " + " contains invalid parameter name: " + key, false);
			}

			parseMetaDataParam(key, val);
		}
		if(_format == null)
			setFormatTypeString(null);
	}

	private void parseMetaDataParam(Object key, Object val)
	{
		switch(key.toString()) {
			case DataExpression.READROWPARAM: _dim1 = (Integer) val; break;
			case DataExpression.READCOLPARAM: _dim2 = (Integer) val; break;
			case DataExpression.ROWBLOCKCOUNTPARAM: setBlocksize((Integer) val); break;
			case DataExpression.READNNZPARAM: setNnz(val instanceof Long ? (Long) val : (Integer) val); break;
			case DataExpression.FORMAT_TYPE: setFormatTypeString((String) val); break;
			case DataExpression.DATATYPEPARAM: setDataType(Types.DataType.valueOf(((String) val).toUpperCase())); break;
			case DataExpression.VALUETYPEPARAM: setValueType(Types.ValueType.fromExternalString((String) val)); break;
			case DataExpression.PRIVACY: setPrivacy(PrivacyConstraint.PrivacyLevel.valueOf((String) val)); break;
			case DataExpression.FINE_GRAINED_PRIVACY:  setFineGrainedPrivacy(val.toString()); break;
			case DataExpression.DELIM_DELIMITER: setDelim(val.toString()); break;
			case DataExpression.SCHEMAPARAM: setSchema(val.toString()); break;
			case DataExpression.DELIM_HAS_HEADER_ROW: setHasHeader(true);
			case DataExpression.DELIM_SPARSE: setSparseDelim((boolean) val);
		}
	}

	public boolean mtdExists() {
		return _metaObj != null && !_metaObj.isEmpty();
	}

	public CacheableData<?> parseAndSetPrivacyConstraint(CacheableData<?> cd) throws JSONException {
		return (CacheableData<?>) PrivacyPropagator.parseAndSetPrivacyConstraint(cd, _metaObj);
	}

	public String getFormatTypeString() {
		return _formatTypeString;
	}

	public String getFineGrainedPrivacy() {
		return _fineGrainedPrivacy;
	}

	public String getDelim() {
		return _delim;
	}

	public String getSchema() {
		return _schema;
	}

	public boolean getHasHeader() {
		return _hasHeader;
	}

	public boolean getSparseDelim() {
		return _sparseDelim;
	}

	public void setSparseDelim(boolean sparseDelim) {
		_sparseDelim = sparseDelim;
	}

	public void setHasHeader(boolean hasHeader) {
		_hasHeader = hasHeader;
	}

	public void setFineGrainedPrivacy(String fineGrainedPrivacy) {
		_fineGrainedPrivacy = fineGrainedPrivacy;
	}

	public void setSchema(String schema) {
		_schema = schema;
	}

	public void setDelim(String delim) {
		_delim = delim;
	}

	public void setFormatTypeString(String format) {
		_formatTypeString = _formatTypeString != null && format == null && _metaObj != null ? (String)JSONHelper.get(_metaObj, DataExpression.FORMAT_TYPE) : format ;
		if(_formatTypeString != null && EnumUtils.isValidEnum(Types.FileFormat.class, _formatTypeString.toUpperCase()))
			setFileFormat(Types.FileFormat.safeValueOf(_formatTypeString));
	}
	
	public DataCharacteristics getDataCharacteristics() {
		return new MatrixCharacteristics(getDim1(), getDim2(), getBlocksize(), getNnz());
	}

	@SuppressWarnings("unchecked")
	public HashMap<String, Expression> parseMetaDataFileParameters(String mtdFileName, boolean conditional, HashMap<String, Expression> varParams)
	{
		for( Object obj : _metaObj.entrySet() ){
			Map.Entry<Object,Object> e = (Map.Entry<Object, Object>) obj;
			Object key = e.getKey();
			Object val = e.getValue();

			boolean isValidName = DataExpression.READ_VALID_MTD_PARAM_NAMES.contains(key);

			if (!isValidName){ //wrong parameters always rejected
				raiseValidateError("MTD file " + mtdFileName + " contains invalid parameter name: " + key, false);
			}

			parseMetaDataParam(key, val);

			// if the read method parameter is a constant, then verify value matches MTD metadata file
			if (varParams.get(key.toString()) != null && (varParams.get(key.toString()) instanceof ConstIdentifier)
				&& !varParams.get(key.toString()).toString().equalsIgnoreCase(val.toString())) {
				raiseValidateError("Parameter '" + key.toString()
					+ "' has conflicting values in metadata and read statement. MTD file value: '"
					+ val.toString() + "'. Read statement value: '" + varParams.get(key.toString()) + "'.", conditional);
			} else {
				// if the read method does not specify parameter value, then add MTD metadata file value to parameter list
				if (varParams.get(key.toString()) == null){
					if (( !key.toString().equalsIgnoreCase(DataExpression.DESCRIPTIONPARAM) ) &&
						( !key.toString().equalsIgnoreCase(DataExpression.AUTHORPARAM) ) &&
						( !key.toString().equalsIgnoreCase(DataExpression.CREATEDPARAM) ) )
					{
						StringIdentifier strId = new StringIdentifier(val.toString(), this);

						if ( key.toString().equalsIgnoreCase(DataExpression.DELIM_HAS_HEADER_ROW)
							|| key.toString().equalsIgnoreCase(DataExpression.DELIM_FILL)
							|| key.toString().equalsIgnoreCase(DataExpression.DELIM_SPARSE)
						) {
							// parse these parameters as boolean values
							BooleanIdentifier boolId = null;
							if (strId.toString().equalsIgnoreCase("true")) {
								boolId = new BooleanIdentifier(true, this);
							} else if (strId.toString().equalsIgnoreCase("false")) {
								boolId = new BooleanIdentifier(false, this);
							} else {
								raiseValidateError("Invalid value provided for '" + DataExpression.DELIM_HAS_HEADER_ROW + "' in metadata file '" + mtdFileName + "'. "
									+ "Must be either TRUE or FALSE.", conditional);
							}
							varParams.remove(key.toString());
							addVarParam(key.toString(), boolId, varParams);

							switch(key.toString().toUpperCase()) {
								case DataExpression.DELIM_HAS_HEADER_ROW: ;
								case DataExpression.DELIM_FILL: ;
								case DataExpression.DELIM_SPARSE: ;
							}

						}
						else if ( key.toString().equalsIgnoreCase(DataExpression.DELIM_FILL_VALUE)) {
							// parse these parameters as numeric values
							DoubleIdentifier doubleId = new DoubleIdentifier(Double.parseDouble(strId.toString()),
								this);
							varParams.remove(key.toString());
							addVarParam(key.toString(), doubleId, varParams);
						}
						else if (key.toString().equalsIgnoreCase(DataExpression.DELIM_NA_STRINGS)
							|| key.toString().equalsIgnoreCase(DataExpression.PRIVACY)
							|| key.toString().equalsIgnoreCase(DataExpression.FINE_GRAINED_PRIVACY)) {
							String naStrings = null;
							if ( val instanceof String) {
								naStrings = val.toString();
							}
							else if (val instanceof JSONArray) {
								StringBuilder sb = new StringBuilder();
								JSONArray valarr = (JSONArray)val;
								for(int naid=0; naid < valarr.size(); naid++ ) {
									sb.append( (String) valarr.get(naid) );
									if ( naid < valarr.size()-1)
										sb.append( DataExpression.DELIM_NA_STRING_SEP );
								}
								naStrings = sb.toString();
							}
							else if ( val instanceof JSONObject ){
								JSONObject valJsonObject = (JSONObject)val;
								naStrings = valJsonObject.toString();
							}
							else {
								throw new ParseException("Type of value " + val
									+ " from metadata not recognized by parser.");
							}
							StringIdentifier sid = new StringIdentifier(naStrings, this);
							varParams.remove(key.toString());
							addVarParam(key.toString(), sid, varParams);
						}
						else {
							// by default, treat a parameter as a string
							addVarParam(key.toString(), strId, varParams);
						}
					}
				}
			}

			if(_format == null)
				setFormatTypeString(null);
		}
		return varParams;
	}

	public void addVarParam(String name, Expression value, HashMap<String, Expression> varParams) {
		if (DMLScript.VALIDATOR_IGNORE_ISSUES && (value == null)) {
			return;
		}
		varParams.put(name, value);

		// if required, initialize values
		setFilename(value.getFilename());
		if (getBeginLine() == 0) setBeginLine(value.getBeginLine());
		if (getBeginColumn() == 0) setBeginColumn(value.getBeginColumn());
		if (getEndLine() == 0) setEndLine(value.getEndLine());
		if (getEndColumn() == 0) setEndColumn(value.getEndColumn());
		if (getText() == null) setText(value.getText());
	}

	public static String checkHasDelimitedFormat(String filename, boolean conditional) {
		// if the MTD file exists, check the format is not binary
		MetaDataAll mtdObject = new MetaDataAll(filename + ".mtd", conditional, false);
		if (mtdObject.mtdExists()) {
			try {
				mtdObject.setFormatTypeString((String) mtdObject._metaObj.get(DataExpression.FORMAT_TYPE));
				if(Types.FileFormat.isDelimitedFormat(mtdObject.getFormatTypeString()))
					return mtdObject.getFormatTypeString();
			}
			catch(JSONException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public static boolean checkHasMatrixMarketFormat(String inputFileName, String mtdFileName, boolean conditional)
	{
		// Check the MTD file exists. if there is an MTD file, return false.
		MetaDataAll mtdObject = new MetaDataAll(mtdFileName, conditional, false);
		if (mtdObject.mtdExists())
			return false;

		if( HDFSTool.existsFileOnHDFS(inputFileName)
			&& !HDFSTool.isDirectory(inputFileName)  )
		{
			Path path = new Path(inputFileName);
			try( BufferedReader in = new BufferedReader(new InputStreamReader(
				IOUtilFunctions.getFileSystem(path).open(path))))
			{
				String headerLine = new String("");
				if (in.ready())
					headerLine = in.readLine();
				return (headerLine !=null && headerLine.startsWith("%%"));
			}
			catch(Exception ex) {
				throw new LanguageException("Failed to read matrix market header.", ex);
			}
		}
		return false;
	}
}
