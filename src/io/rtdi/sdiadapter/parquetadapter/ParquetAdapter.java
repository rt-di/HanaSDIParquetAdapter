/**
 * (c) 2017 SAP SE or an SAP affiliate company. All rights reserved.
 */
package io.rtdi.sdiadapter.parquetadapter;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterAdmin;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.CallableProcedure;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.DataDictionary;
import com.sap.hana.dp.adapter.sdk.FunctionMetadata;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.ParametersResponse;
import com.sap.hana.dp.adapter.sdk.ProcedureMetadata;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteObjectsFilter;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.parser.ColumnReference;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase.Type;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;


/**
*	ParquetAdapter Adapter.
*/
public class ParquetAdapter extends Adapter{

	static Logger logger = LogManager.getLogger("ParquetAdapter");
	private int arraysize = 1000;
	private String currentbrowsenode = null;
	private RemoteObjectsFilter browsefilter = null;
	private AdapterException connectionexception;
	private Configuration conf;
	private ParquetProducer producer;
	private ArrayBlockingQueue<AdapterRow> queue;
	private String rooturl;
	private long limit;
	private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml"};
	
	@Override
	public void beginTransaction() throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
		String startpoint = this.currentbrowsenode;
		if (startpoint == null) {
			startpoint = rooturl;
		}
		Configuration conf = new Configuration();
		try (FileSystem fs = FileSystem.get(conf);) {
			FileStatus[] files = fs.listStatus(new Path(startpoint));
			for (FileStatus file : files ) {
				Path path = file.getPath();
				BrowseNode node = new BrowseNode(path.toString(), path.getName());
				if (file.isDirectory()) {
					node.setImportable(true);
					node.setExpandable(true);
				} else {
					node.setImportable(true);
					node.setExpandable(false);
				}
				nodes.add(node);
			}
			return nodes;
		} catch (IOException e) {
			throw new AdapterException(e);
		}
	}

	@Override
	public void close() throws AdapterException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commitTransaction() throws AdapterException {
		// TODO Auto-generated method stub
		
	}
	
	private class ParquetProducer extends Thread {
		
		private String nodeid;
		private Exception lastexception = null;
		private List<String> projection;
		private AdapterRowSet rowset = null;
		private long rowcount;


		private ParquetProducer(String nodeid, List<String> projection) {
			this.nodeid = nodeid;
			this.projection = projection;
		}

		@Override
		public void run() {
			try {
				rowcount = 0;
				List<HadoopInputFile> filestoread = getFiles(nodeid, conf);
				for (HadoopInputFile file : filestoread) {
					ParquetReadOptions options = ParquetReadOptions.builder().build();
					try (ParquetFileReader reader = ParquetFileReader.open(file, options);) {
						MessageType schema = reader.getFileMetaData().getSchema();
						
						MessageTypeBuilder projectionbuilder = Types.buildMessage();
						
						Mapping mapping = new Mapping(schema, projectionbuilder, null);
						for (int colindex=0; colindex<projection.size(); colindex++) {
							String fieldname = projection.get(colindex);
							String [] path = fieldname.split("\\.");
							mapping.add(path, colindex, 0);
						}
						
						
						MessageType projectionschema = (MessageType) mapping.createProjection();
						
						MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projectionschema);
						PageReadStore pagestore;
						Object[] currentrow = new Object[projection.size()];
						while (null != (pagestore = reader.readNextRowGroup()) && !isInterrupted() && rowcount<limit) {
							RecordReader<Group> recordReader = columnIO.getRecordReader(pagestore, new GroupRecordConverter(projectionschema));
							
							
							
							long rows = pagestore.getRowCount();
							for (int i = 0; i < rows; i++) {
								Group group = recordReader.read();
								mapping.applyRecords(group, currentrow);
							}
						}
					}
				}
			} catch (IOException | AdapterException | InterruptedException e) {
				lastexception = e;
			}
		}

		public void setRowSet(AdapterRowSet rows) {
			this.rowset = rows;
		}

		public boolean hasRowSet() {
			return this.rowset != null;
		}
		
		private class Mapping {
			private Map<Integer, String> colindex;
			private GroupType schema;
			private Map<String, Mapping> childmappings = new HashMap<>();
			private List<Mapping> childmapping = new ArrayList<>();
			private GroupBuilder<?> projectionbuilder;
			private GroupType projectionschema;
			private Mapping outputmapping;
			private Mapping parent;
			
			public Mapping(GroupType schema, GroupBuilder<?> projectionbuilder, Mapping parent) {
				this.schema = schema;
				this.projectionbuilder = projectionbuilder;
				this.parent = parent;
			}

			public void applyRecords(Group group, Object[] currentrecord) throws AdapterException, InterruptedException {
				join(group, 0, currentrecord, 0);
			}

			private int join(Group group, int table, Object[] currentrecord, int depth) throws AdapterException, InterruptedException {
				if (colindex != null) {
					for (Integer i : colindex.keySet()) {
						String field = colindex.get(i);
						int groupindex = projectionschema.getFieldIndex(field);
						currentrecord[i] = getValue(group, groupindex, schema.getType(field));
					}
					depth += colindex.size();
				}
				if (table < childmapping.size()) {
					Mapping m = childmapping.get(table);
					for (int rows=0; rows < group.getFieldRepetitionCount(m.schema.getName()) && rowcount<limit; rows++) {
						Group g = group.getGroup(m.schema.getName(), rows);
						int d = m.join(g, 0, currentrecord, depth);
						if (childmappings.size() > table+1) {
							join(g, table+1, currentrecord, d);
						}
					}
				} else if (depth == currentrecord.length) {
					AdapterRow row = rowset.getTemplateRow();
					for (int i=0; i<currentrecord.length; i++) {
						if (currentrecord[i] instanceof Integer) {
							row.setColumnValue(i, (Integer) currentrecord[i]);
						} else if (currentrecord[i] instanceof Long) {
							row.setColumnValue(i, (Long) currentrecord[i]);
						} else if (currentrecord[i] instanceof String) {
							row.setColumnValue(i, (String) currentrecord[i]);
						} else if (currentrecord[i] instanceof BigInteger) {
							row.setColumnValue(i, new BigDecimal((BigInteger) currentrecord[i]));
						} else if (currentrecord[i] instanceof BigDecimal) {
							row.setColumnValue(i, (BigDecimal) currentrecord[i]);
						} else if (currentrecord[i] instanceof byte[]) {
							row.setColumnValue(i, (byte[]) currentrecord[i]);
						} else if (currentrecord[i] instanceof Double) {
							row.setColumnValue(i, (Double) currentrecord[i]);
						}
					}
					queue.put(row);
					rowcount++;
				}
				return depth;
			}

			public void add(String[] path, int projectionindex, int level) {
				String p = path[level];
				if (level < path.length-1) {
					if (p.endsWith("[]")) {
						p = p.substring(0, p.indexOf("[]"));
					}
					Mapping m = childmappings.get(p);
					GroupType s = schema.getFields().get(schema.getFieldIndex(p)).asGroupType();
					if (m == null) {
						GroupBuilder<?> gbc = projectionbuilder.group(s.getRepetition());
						gbc.as(s.getOriginalType());
						m = new Mapping(s, gbc, this);
						addChild(m);
						if (s.getOriginalType() == OriginalType.LIST) {
							GroupType s1 = s.getFields().get(0).asGroupType();
							GroupBuilder<?> gbc1 = gbc.group(s1.getRepetition());
							Mapping m1 = new Mapping(s1, gbc1, m);
							m.addChild(m1);
							GroupType s2 = s1.getFields().get(0).asGroupType();
							GroupBuilder<?> gbc2 = gbc1.group(s2.getRepetition());
							Mapping m2 = new Mapping(s2, gbc2, m1);
							m1.addChild(m2);
							m.outputmapping = m2;
						}
					}
					m.outputmapping.add(path, projectionindex, level+1);
				} else {
					// add a scalar field
					if (colindex == null) {
						colindex = new HashMap<>();
					}
					colindex.put(projectionindex, p);
					projectionbuilder.addField(schema.getFields().get(schema.getFieldIndex(p)));
				}
			}
			
			private void addChild(Mapping m) {
				childmappings.put(m.schema.getName(), m);
				childmapping.add(m);				
			}
			
			
			@SuppressWarnings("deprecation")
			public GroupType createProjection() {
				List<org.apache.parquet.schema.Type> children = new ArrayList<>();
				for (Mapping c : childmapping) {
					children.add(c.createProjection());
				}
				if (this.colindex != null) {
					for (String c : colindex.values()) {
						children.add(schema.getFields().get(schema.getFieldIndex(c)));
					}
				}
				if (parent != null) {
					projectionschema = new GroupType(schema.getRepetition(), schema.getName(), schema.getOriginalType(), children);
				} else {
					projectionschema = new MessageType(schema.getName(), children);
				}
				return projectionschema;
			}
			

			public Object getValue(Group group, int index, org.apache.parquet.schema.Type type) throws AdapterException {
				OriginalType otype = type.getOriginalType();
				if (otype != null) {
					switch (otype) {
					case BSON:
						break;
					case DATE:
						return new Date( ((long) group.getInteger(index, 0)) * 24L * 3600L * 1000L);
					case DECIMAL:
						break;
					case ENUM:
						break;
					case INTERVAL:
						break;
					case INT_16:
						return group.getInteger(index, 0);
					case INT_32:
						return group.getInteger(index, 0);
					case INT_64:
						return group.getLong(index, 0);
					case INT_8:
						return group.getInteger(index, 0);
					case JSON:
						break;
					case LIST:
						break;
					case MAP:
						break;
					case MAP_KEY_VALUE:
						break;
					case TIMESTAMP_MICROS:
						break;
					case TIMESTAMP_MILLIS:
						
					case TIME_MICROS:
						break;
					case TIME_MILLIS:
						break;
					case UINT_16:
						return group.getInteger(index, 0);
					case UINT_32:
						return group.getInteger(index, 0);
					case UINT_64:
						return group.getLong(index, 0);
					case UINT_8:
						return group.getInteger(index, 0);
					case UTF8:
						return group.getString(index, 0);
					default:
						break;
					}
				} else {
					switch (type.asPrimitiveType().getPrimitiveTypeName()) {
					case BINARY:
						return group.getBinary(index, 0).getBytes();
					case BOOLEAN:
						return group.getBoolean(index, 0);
					case DOUBLE:
						return group.getDouble(index, 0);
					case FIXED_LEN_BYTE_ARRAY:
						return group.getBinary(index, 0);
					case FLOAT:
						return group.getFloat(index, 0);
					case INT32:
						return group.getInteger(index, 0);
					case INT64:
						return group.getLong(index, 0);
					case INT96:
						return new BigInteger(group.getBinary(index, 0).getBytes());
					default:
						break;
					}
				}
				return null;
			}
			
		}
		
	}

	@Override
	public void executeStatement(String sql, StatementInfo info) throws AdapterException {
		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
		queue = new ArrayBlockingQueue<>(1000);
		ExpressionBase b = ExpressionParserUtil.buildQuery(sql, messageList);
		if (b != null) {
			if (b.getType() == Type.SELECT) {
				Query query = (Query) b;
				TableReference table = (TableReference) query.getFromClause();
				try {
					// reader = ParquetReader.builder(new GroupReadSupport(), new Path(table.getUnquotedFullName())).build();
					List<ExpressionBase> projections = query.getProjections();
					List<String> projection = new ArrayList<>(projections.size());
					for (ExpressionBase p : projections) {
						ColumnReference c = (ColumnReference) p;
						projection.add(c.getUnquotedColumnName());
					}
					producer = new ParquetProducer(table.getUnquotedFullName(), projection);
					
					if (query.getLimit() != null) {
						this.limit = query.getLimit();
					} else {
						this.limit = Long.MAX_VALUE;
					}
				} catch (IllegalArgumentException e) {
					throw new AdapterException(e);
				}
			} else {
				throw new AdapterException("Adapter supports select statements only");
			}
		} else {
			throw new AdapterException("Pasing the provided SQL failed with error \"" + messageList.toString() + "\"");
		}
	}

	@Override
	public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
		Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
		capbility.setCapability(AdapterCapability.CAP_SELECT);
		capbility.setCapability(AdapterCapability.CAP_LIMIT);
		capbility.setCapability(AdapterCapability.CAP_LIMIT_ARG);
		capbility.setCapability(AdapterCapability.CAP_PROJECT);
		capbility.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
		capbility.setCapability(AdapterCapability.CAP_WHERE);
		capbility.setCapability(AdapterCapability.CAP_AGGREGATES);
		capbility.setCapability(AdapterCapability.CAP_BETWEEN);
		capbility.setCapability(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		capbility.setCapability(AdapterCapability.CAP_BIGINT_BIND);
		capbility.setCapability(AdapterCapability.CAP_GROUPBY);
		capbility.setCapability(AdapterCapability.CAP_GROUPBY_ALL);
		capbility.setCapability(AdapterCapability.CAP_IN);
		return capbility;
	}

	@Override
	public int getLob(long lobId, byte[] bytes, int bufferSize) throws AdapterException {
		return 0;
	}
	
	@Override
	public void getNext(AdapterRowSet rows) throws AdapterException {
		if (!producer.hasRowSet()) {
			producer.setRowSet(rows); // add metadata about the row structure. It is stable across multiple calls
			producer.start();
		}
		int rowcount = 0;
		try {
			while (rowcount < arraysize && (producer.isAlive() || queue.size() != 0)) {
				AdapterRow row = queue.poll(5, TimeUnit.SECONDS);
				if (row != null) {
					rows.addRow(row);
					rowcount++;
				}
			}
		} catch (InterruptedException e) {
			throw new AdapterException(e);
		}
	}

	@Override
	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();

		PropertyGroup connectionInfo = new PropertyGroup("connectioninfo","HDFS Parameters","HDFS Parameters");

		PropertyEntry rooturl = new PropertyEntry("rooturl", "Root Directory URL", "The root directory as hdfs url");
		rooturl.setDefaultValue("./data");
		connectionInfo.addProperty(rooturl);

		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry("credential", "Credentials as specified in the adapter config");
		credential.getUser().setDisplayName("Username");
		credential.getPassword().setDisplayName("Password");
		credentialProperties.addCredentialEntry(credential);

		rs.setCredentialProperties(credentialProperties);
		rs.setConnectionProperties(connectionInfo);
		return rs;
	}
	

	@Override
	public String getSourceVersion(RemoteSourceDescription remoteSourceDescription) throws AdapterException {
		return "1";
	}

	@Override
	public Metadata importMetadata(String nodeId) throws AdapterException {

		try {
			List<HadoopInputFile> files = getFiles(nodeId, conf);
			HadoopInputFile file = files.get(0);

			ParquetReadOptions options = ParquetReadOptions.builder().build();
			
			try (ParquetFileReader reader = ParquetFileReader.open(file, options);) {
				MessageType schema = reader.getFileMetaData().getSchema();
				List<Column> columns = new ArrayList<Column>();
				List<org.apache.parquet.schema.Type> parquetcolumns = schema.getFields();
				addColumns(parquetcolumns, columns, null, false);
				TableMetadata table = new TableMetadata();
				table.setName(nodeId);
				table.setColumns(columns);
				return table;
			}
		} catch (IOException e) {
			throw new AdapterException(e);
		}
	}
	
	private void addColumns(List<org.apache.parquet.schema.Type> parquetcolumns, List<Column> columns, String parent, boolean parentislist) {
		for (org.apache.parquet.schema.Type parquetcolumn : parquetcolumns) {
			String columnname;
			if (parent == null) {
				columnname = parquetcolumn.getName();
			} else {
				columnname = parent + "." + parquetcolumn.getName();
			}
			if (parquetcolumn instanceof GroupType) {
				GroupType group = (GroupType) parquetcolumn;
				OriginalType otype = group.getOriginalType();
				if (otype == OriginalType.LIST) { // hide the "list" group
					addColumns(group.getFields(), columns, columnname + "[]", true); 
				} else if (group.isRepetition(Repetition.REPEATED)) { // hide the list.element group
					addColumns(group.getFields(), columns, parent, parentislist);
				} else if (parentislist) { // hide the list.element group
					addColumns(group.getFields(), columns, parent, false);
				} else {
					addColumns(group.getFields(), columns, columnname, false);
				}
			} else {
				Column col = new Column(columnname, DataType.VARCHAR, 256);
				col.setNullable(true);
				columns.add(col);
			}
		}
	}

	private List<HadoopInputFile> getFiles(String nodeId, Configuration conf) throws IOException {
		Path path = new Path(nodeId);
		try (FileSystem fs = FileSystem.get(conf);) {
			List<HadoopInputFile> ret = new ArrayList<>();
			FileStatus status = fs.getFileStatus(path);
			if (status.isDirectory()) {
				List<Path> files = getInputFilesFromDirectory(path, conf);
				for (Path p : files) {
					ret.add(HadoopInputFile.fromPath(p, conf));
				}
			} else {
				ret.add(HadoopInputFile.fromPath(path, conf));
			}
			return ret;			
		}

	}

	private List<Path> getInputFilesFromDirectory(Path partitionDir, Configuration conf) throws IOException {
		FileSystem fs = partitionDir.getFileSystem(conf);
		FileStatus[] inputFiles = fs.listStatus(partitionDir, HiddenFileFilter.INSTANCE);

		List<Path> input = new ArrayList<Path>();
		for (FileStatus f: inputFiles) {
			input.add(f.getPath());
		}
		return input;
	}
	  
	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC) throws AdapterException {
		PropertyEntry node = connectionInfo.getConnectionProperties().getPropertyEntry(ParquetAdapterFactory.ROOTURL);
		this.rooturl = node.getValue();

		conf = new Configuration();
		
		try {
			PropertyGroup adapterconf = AdapterAdmin.getAdapterConfiguration("ParquetAdapter");

			String username = new String(connectionInfo.getCredentialProperties().getCredentialEntry(ParquetAdapterFactory.CREDENTIAL).getUser().getValue(), "UTF-8");
			String password = new String(connectionInfo.getCredentialProperties().getCredentialEntry(ParquetAdapterFactory.CREDENTIAL).getPassword().getValue(), "UTF-8");

			String confdir = adapterconf.getPropertyEntry(ParquetAdapterFactory.CONFDIR).getValue();
			if (confdir != null) {
				for (String file : HADOOP_CONF_FILES) {
					File f = new File(confdir, file);
					if (f.exists()) {
						conf.addResource(new Path(f.getAbsolutePath()));
						logger.info("Added configuration file \"" + f.getAbsolutePath() + "\" to the Hadoop configuration");
					} else {
						logger.info("No configuration file found at \"" + f.getAbsolutePath() + "\"");
					}
				}
			} else {
				logger.info("No Haddop client configuration directory specified in the adapter preferences, hence none used");
			}

		} catch (UnsupportedEncodingException e1) {
			throw new AdapterException(e1, e1.getMessage());
		}
	}
		
	
	@Override
	public int putNext(AdapterRowSet rows) throws AdapterException {
		return 0;
	}
	
	@Override
	public void rollbackTransaction() throws AdapterException {
	}
	
	@Override
	public void setBrowseNodeId(String nodeId) throws AdapterException {
		currentbrowsenode = nodeId;
	}
	
	@Override
	public void setFetchSize(int fetchSize) {
		arraysize = fetchSize;
	}
	
	@Override
	public void setAutoCommit(boolean autocommit) throws AdapterException {
	}
	
	@Override
	public void executePreparedInsert(String arg0, StatementInfo arg1) throws AdapterException {
	}

	@Override
	public void executePreparedUpdate(String arg0, StatementInfo arg1) throws AdapterException {
	}

	@Override
	public int executeUpdate(String sql, StatementInfo info) throws AdapterException {
		return 0;
	}
	
	@Override
	public Metadata importMetadata(String nodeId, List<Parameter> dataprovisioningParameters) throws AdapterException {
		return null;
	}
	
	@Override
	public ParametersResponse queryParameters(String nodeId, List<Parameter> parametersValues) throws AdapterException {
		return null;
	}
	
	@Override
	public List<BrowseNode> loadTableDictionary(String lastUniqueName) throws AdapterException {
		return null;
	}
	
	@Override
	public DataDictionary loadColumnsDictionary() throws AdapterException {
		return null;
	}
		
	@Override
	public void executeCall(FunctionMetadata metadata) throws AdapterException{
	}
	
	@Override
	public void validateCall(FunctionMetadata metadata) throws AdapterException{
	}
	
	@Override
	public void setNodesListFilter(RemoteObjectsFilter remoteObjectsFilter) throws AdapterException {
    }
	

	@Override
	public Metadata getMetadataDetail(String nodeId) throws AdapterException {
		return null;
	}

	@Override
	public CallableProcedure prepareCall(ProcedureMetadata metadata) throws AdapterException {
		return null;
	}
	
	@Override
	public void closeResultSet() throws AdapterException {
		if (producer != null) {
			if (producer.isAlive()) {
				producer.interrupt();
			}
			producer = null;
		}
	}


}
