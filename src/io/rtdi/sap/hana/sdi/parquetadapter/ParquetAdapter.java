package io.rtdi.sap.hana.sdi.parquetadapter;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.*;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.BsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem;
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
public class ParquetAdapter extends Adapter {

	static Logger logger = LogManager.getLogger("ParquetAdapter");
	private int arraysize = 1000;
	private String currentbrowsenode = null;
	static Configuration conf;
	private ParquetProducer producer;
	private ArrayBlockingQueue<AdapterRow> queue;
	private String rooturl;
	private long limit;
	private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml"};
	
	static {
		conf = new Configuration();
		conf.setClass("fs.file.impl", BareLocalFileSystem.class, FileSystem.class);
	}
	
	@Override
	public void open(RemoteSourceDescription connectionInfo, boolean isCDC) throws AdapterException {
		PropertyEntry node = connectionInfo.getConnectionProperties().getPropertyEntry(ParquetAdapterFactory.ROOTURL);
		this.rooturl = node.getValue();
		
		try {
			PropertyGroup adapterconf = AdapterAdmin.getAdapterConfiguration("ParquetAdapter");
			CredentialProperties adaptercredentials = AdapterAdmin.getSecureAdapterConfiguration("ParquetAdapter");

			String username = new String(connectionInfo.getCredentialProperties().getCredentialEntry(ParquetAdapterFactory.CREDENTIAL).getUser().getValue(), "UTF-8");
			String password = new String(connectionInfo.getCredentialProperties().getCredentialEntry(ParquetAdapterFactory.CREDENTIAL).getPassword().getValue(), "UTF-8");

			String adapterusername = new String(adaptercredentials.getCredentialEntry(ParquetAdapterFactory.CREDENTIAL).getUser().getValue(), "UTF-8");
			String adapterpassword = new String(adaptercredentials.getCredentialEntry(ParquetAdapterFactory.CREDENTIAL).getPassword().getValue(), "UTF-8");

			if (!username.equals(adapterusername) || !password.equals(adapterpassword)) {
				throw new AdapterException("Remote source credentials must match the configured values in the ParquetAdapter - see ./bin/agentcli --configAdapters");
			}
			

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
				logger.info("No Hadoop client configuration directory specified in the adapter preferences, hence none used");
			}

		} catch (UnsupportedEncodingException | RuntimeException e) {
			throw new AdapterException(e, e.getMessage());
		}
	}


	@Override
	public List<BrowseNode> browseMetadata() throws AdapterException {
		List<BrowseNode> nodes = new ArrayList<BrowseNode>();
		String startpoint = this.currentbrowsenode;
		if (startpoint == null) {
			startpoint = rooturl;
		}
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
	}

	@Override
	public void commitTransaction() throws AdapterException {
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
					List<ExpressionBase> projections = query.getProjections();
					List<String> projection = new ArrayList<>(projections.size());
					for (ExpressionBase p : projections) {
						ColumnReference c = (ColumnReference) p;
						projection.add(c.getUnquotedColumnName());
					}
					if (query.getLimit() != null) {
						this.limit = query.getLimit();
					} else {
						this.limit = Long.MAX_VALUE;
					}
					producer = new ParquetProducer(table.getUnquotedFullName(), projection, queue, limit, logger);
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
		// capbility.setCapability(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
		// capbility.setCapability(AdapterCapability.CAP_WHERE);
		// capbility.setCapability(AdapterCapability.CAP_AGGREGATES);
		// capbility.setCapability(AdapterCapability.CAP_BETWEEN);
		// capbility.setCapability(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);
		capbility.setCapability(AdapterCapability.CAP_BIGINT_BIND);
		// capbility.setCapability(AdapterCapability.CAP_GROUPBY);
		// capbility.setCapability(AdapterCapability.CAP_GROUPBY_ALL);
		// capbility.setCapability(AdapterCapability.CAP_IN);
		return capbility;
	}

	@Override
	public int getLob(long lobId, byte[] bytes, int bufferSize) throws AdapterException {
		return 0;
	}
	
	@Override
	public void getNext(AdapterRowSet rows) throws AdapterException {
		if (!producer.hasRowSet()) {
			logger.debug("Starting producer");
			producer.setRowSet(rows); // add metadata about the row structure. It is stable across multiple calls
			producer.start();
		}
		int rowcount = 0;
		try {
			logger.debug("Polling for data: rowcount {}, exception {}, alive {}, queue size {}", rowcount, producer.lastexception, producer.isAlive(), queue.size());
			while (rowcount < arraysize && producer.lastexception == null && (producer.isAlive() || queue.size() != 0)) {
				AdapterRow row = queue.poll(5, TimeUnit.SECONDS);
				if (row != null) {
					rows.addRow(row);
					rowcount++;
				}
			}
			if (producer.lastexception != null) {
				logger.error("producer ran into an exception");
				throw new AdapterException(producer.lastexception);
			}
		} catch (InterruptedException | RuntimeException e) {
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
				LogicalTypeAnnotation otype = group.getLogicalTypeAnnotation();
				if (otype == LogicalTypeAnnotation.listType()) { // hide the "list" group
					addColumns(group.getFields(), columns, columnname + "[]", true); 
				} else if (group.isRepetition(Repetition.REPEATED)) { // hide the list.element group
					addColumns(group.getFields(), columns, parent, parentislist);
				} else if (parentislist) { // hide the list.element group
					addColumns(group.getFields(), columns, parent, false);
				} else {
					addColumns(group.getFields(), columns, columnname, false);
				}
			} else {
				LogicalTypeAnnotation otype = parquetcolumn.getLogicalTypeAnnotation();
				PrimitiveTypeName primitive = parquetcolumn.asPrimitiveType().getPrimitiveTypeName();
				Column col = null;
				if (otype == null) {
					/*
					 * In case there is no specific logical type, fall back and return the primitive value.
					 */
					switch (primitive) {
					case BINARY:
						col = new Column(columnname, DataType.BLOB);
						break;
					case BOOLEAN:
						col = new Column(columnname, DataType.BOOLEAN);
						break;
					case DOUBLE:
						col = new Column(columnname, DataType.DOUBLE);
						break;
					case FIXED_LEN_BYTE_ARRAY:
						col = new Column(columnname, DataType.VARBINARY, 1024);
						break;
					case FLOAT:
						col = new Column(columnname, DataType.REAL);
						break;
					case INT32:
						col = new Column(columnname, DataType.INTEGER);
						break;
					case INT64:
						col = new Column(columnname, DataType.BIGINT);
						break;
					case INT96:
						/*
						 * Deprecated timestamp 
						 */
						col = new Column(columnname, DataType.TIMESTAMP);
						break;
					default:
						break;
					}
				} else if (otype instanceof BsonLogicalTypeAnnotation) {
				} else if (otype instanceof IntLogicalTypeAnnotation) {
					switch (primitive) {
					case INT32:
						col = new Column(columnname, DataType.INTEGER);
						break;
					case INT64:
						col = new Column(columnname, DataType.BIGINT);
						break;
					default:
						break;
					}
				} else if (otype instanceof StringLogicalTypeAnnotation) {
					col = new Column(columnname, DataType.NVARCHAR, 1024);
				} else if (otype instanceof MapLogicalTypeAnnotation) {
				} else if (otype instanceof EnumLogicalTypeAnnotation) {
					col = new Column(columnname, DataType.VARCHAR, 256);
				} else if (otype instanceof DecimalLogicalTypeAnnotation) {
					DecimalLogicalTypeAnnotation dectype = (DecimalLogicalTypeAnnotation) otype;
					col = new Column(columnname, DataType.DECIMAL, dectype.getPrecision(), dectype.getScale());
				} else if (otype instanceof DateLogicalTypeAnnotation) {
					col = new Column(columnname, DataType.DATE);
				} else if (otype instanceof TimeLogicalTypeAnnotation) {
					col = new Column(columnname, DataType.TIME);
				} else if (otype instanceof TimestampLogicalTypeAnnotation) {
					col = new Column(columnname, DataType.TIMESTAMP);
				} else if (otype instanceof JsonLogicalTypeAnnotation) {
					col = new Column(columnname, DataType.NVARCHAR, 4096);
				} else if (otype instanceof UUIDLogicalTypeAnnotation) {
					col = new Column(columnname, DataType.VARCHAR, 64);
				}
				if (col != null) {
					col.setNullable(parquetcolumn.getRepetition() == Repetition.OPTIONAL);
					columns.add(col);
				}
			}
		}
	}

	static List<HadoopInputFile> getFiles(String nodeId, Configuration conf) throws IOException {
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

	private static List<Path> getInputFilesFromDirectory(Path partitionDir, Configuration conf) throws IOException {
		FileSystem fs = partitionDir.getFileSystem(conf);
		FileStatus[] inputFiles = fs.listStatus(partitionDir, HiddenFileFilter.INSTANCE);

		List<Path> input = new ArrayList<Path>();
		for (FileStatus f: inputFiles) {
			input.add(f.getPath());
		}
		return input;
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
	public void beginTransaction() throws AdapterException {
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
