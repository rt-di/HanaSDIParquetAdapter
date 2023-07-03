package io.rtdi.sap.hana.sdi.parquetadapter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.logging.log4j.Logger;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
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
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.Timestamp;

class ParquetProducer extends Thread {
	
	private String nodeid;
	Exception lastexception = null;
	private List<String> projection;
	private AdapterRowSet rowset = null;
	private long rowcount;
	private ArrayBlockingQueue<AdapterRow> queue;
	private long limit;
	private Logger logger;


	ParquetProducer(String nodeid, List<String> projection, ArrayBlockingQueue<AdapterRow> queue, long limit, Logger logger) {
		this.nodeid = nodeid;
		this.projection = projection;
		this.queue = queue;
		this.limit = limit;
		this.logger = logger;
	}

	@Override
	public void run() {
		try {
			rowcount = 0;
			List<HadoopInputFile> filestoread = ParquetAdapter.getFiles(nodeid, ParquetAdapter.conf);
			for (HadoopInputFile file : filestoread) {
				ParquetReadOptions options = ParquetReadOptions.builder().build();
				logger.debug("Reading file {}", file.toString());
				try (ParquetFileReader reader = ParquetFileReader.open(file, options);) {
					MessageType schema = reader.getFileMetaData().getSchema();
					
					MessageTypeBuilder projectionbuilder = Types.buildMessage();
					
					Mapping mapping = new Mapping(schema, projectionbuilder, null, logger);
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
							SimpleGroup group = (SimpleGroup) recordReader.read();
							mapping.applyRecords(group, currentrow);
						}
					}
				}
				logger.debug("Reading file {} completed, rowcount {}", file.toString(), rowcount);
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
		private Logger logger;
		
		public Mapping(GroupType schema, GroupBuilder<?> projectionbuilder, Mapping parent, Logger logger) {
			this.schema = schema;
			this.projectionbuilder = projectionbuilder;
			this.parent = parent;
			this.logger = logger;
		}

		public void applyRecords(Group group, Object[] currentrecord) throws AdapterException, InterruptedException {
			join(group, 0, currentrecord, 0);
		}

		private int join(Group group, int table, Object[] currentrecord, int depth) throws AdapterException, InterruptedException {
			if (colindex != null) {
				for (Integer i : colindex.keySet()) {
					String field = colindex.get(i);
					int groupindex = projectionschema.getFieldIndex(field);
					currentrecord[i] = getValue((SimpleGroup) group, groupindex);
				}
				depth += colindex.size();
			}
			if (table < childmapping.size()) {
				Mapping m = childmapping.get(table);
				for (int rows=0; rows < group.getFieldRepetitionCount(m.schema.getName()) && rowcount<limit; rows++) {
					SimpleGroup g = (SimpleGroup) group.getGroup(m.schema.getName(), rows);
					int d = m.join(g, 0, currentrecord, depth);
					if (childmappings.size() > table+1) {
						join(g, table+1, currentrecord, d);
					}
				}
			} else if (depth == currentrecord.length) {
				AdapterRow row = rowset.getTemplateRow();
				for (int i=0; i<currentrecord.length; i++) {
					/*
					 * Unfortunately, there is no AdapterRow method to set the value based on the object itself, hence the correct method overload must
					 * be called.
					 */
					if (currentrecord[i] == null) {
						row.setColumnNull(i);
					} else if (currentrecord[i] instanceof Integer) {
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
					} else if (currentrecord[i] instanceof Timestamp) {
						row.setColumnValue(i, (Timestamp) currentrecord[i]);
					} else if (currentrecord[i] instanceof Boolean) {
						row.setColumnValue(i, (Boolean) currentrecord[i]);
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
					gbc.as(s.getLogicalTypeAnnotation());
					m = new Mapping(s, gbc, this, logger);
					addChild(m);
					if (s.getLogicalTypeAnnotation() == LogicalTypeAnnotation.listType()) {
						GroupType s1 = s.getFields().get(0).asGroupType();
						GroupBuilder<?> gbc1 = gbc.group(s1.getRepetition());
						Mapping m1 = new Mapping(s1, gbc1, m, logger);
						m.addChild(m1);
						GroupType s2 = s1.getFields().get(0).asGroupType();
						GroupBuilder<?> gbc2 = gbc1.group(s2.getRepetition());
						Mapping m2 = new Mapping(s2, gbc2, m1, logger);
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
				projectionschema = new GroupType(schema.getRepetition(), schema.getName(), children);
			} else {
				projectionschema = new MessageType(schema.getName(), children);
			}
			return projectionschema;
		}
		

		public Object getValue(SimpleGroup group, int index) throws AdapterException {
			org.apache.parquet.schema.Type type = group.getType().getType(index);
			LogicalTypeAnnotation otype = type.getLogicalTypeAnnotation();
			PrimitiveTypeName primitive = type.asPrimitiveType().getPrimitiveTypeName();
			int valuecount = group.getFieldRepetitionCount(index);
			if (valuecount == 0) {
				/*
				 * If the group has no values, it is a null.
				 */
				return null;
			} else if (otype == null) {
				/*
				 * In case there is no specific logical type, fall back and return the primitive value.
				 */
				switch (primitive) {
				case BINARY:
					return group.getBinary(index, 0).getBytes();
				case BOOLEAN:
					return group.getBoolean(index, 0);
				case DOUBLE:
					return group.getDouble(index, 0);
				case FIXED_LEN_BYTE_ARRAY:
					return group.getBinary(index, 0).getBytes();
				case FLOAT:
					return group.getFloat(index, 0);
				case INT32:
					return group.getInteger(index, 0);
				case INT64:
					return group.getLong(index, 0);
				case INT96:
					NanoTime nanoTime = NanoTime.fromBinary(group.getInt96(index, 0));
					long nanos = (nanoTime.getJulianDay() - 2440588L) * (86400L * 1000L * 1000L * 1000L) + nanoTime.getTimeOfDayNanos();
					return new Timestamp(nanos / 1000000L);
				default:
					break;
				}
			} else if (otype instanceof BsonLogicalTypeAnnotation) {
				return null;
			} else if (otype instanceof IntLogicalTypeAnnotation) {
				switch (primitive) {
				case INT32:
					return group.getInteger(index, 0);
				case INT64:
					return group.getLong(index, 0);
				default:
					return null;
				}
			} else if (otype instanceof StringLogicalTypeAnnotation) {
				return group.getString(index, 0);
			} else if (otype instanceof MapLogicalTypeAnnotation) {
			} else if (otype instanceof EnumLogicalTypeAnnotation) {
				return group.getString(index, 0);
			} else if (otype instanceof DecimalLogicalTypeAnnotation) {
				DecimalLogicalTypeAnnotation decimaltype = (DecimalLogicalTypeAnnotation) otype;
				BigInteger bigint = null;
				switch (primitive) {
				case FIXED_LEN_BYTE_ARRAY:
				case BINARY:
					bigint = new BigInteger(group.getBinary(index, 0).getBytes());
					break;
				case INT32:
					bigint = BigInteger.valueOf(group.getInteger(index, 0));
					break;
				case INT64:
					bigint = BigInteger.valueOf(group.getLong(index, 0));
					break;
				default:
					return null;
				}
				return new BigDecimal(bigint, decimaltype.getScale());
			} else if (otype instanceof DateLogicalTypeAnnotation) {
				/*
				 * Date is the number of days since 1970.
				 */
				int d = group.getInteger(index, 0);
				LocalDate date = LocalDate.ofEpochDay(d);
				return new Timestamp(date.getDayOfMonth(), date.getMonthValue(), date.getYear());
			} else if (otype instanceof TimeLogicalTypeAnnotation) {
				/*
				 * Depending on the unit, different primitives are annotated
				 */
				TimeLogicalTypeAnnotation timetype = (TimeLogicalTypeAnnotation) otype;
				switch (timetype.getUnit()) {
				case MILLIS:
					return new Timestamp(group.getInteger(index, 0));
				case MICROS:
					return new Timestamp(group.getLong(index, 0)/1000L);
				case NANOS:
					return new Timestamp(group.getLong(index, 0)/1000000L);
				default:
					return null;
				}
			} else if (otype instanceof TimestampLogicalTypeAnnotation) {
				/*
				 * Depending on the unit, the int64 means different things.
				 * Note the timestamp can handle milliseconds only
				 */
				TimestampLogicalTypeAnnotation timetype = (TimestampLogicalTypeAnnotation) otype;
				switch (timetype.getUnit()) {
				case MILLIS:
					return new Timestamp(group.getLong(index, 0));
				case MICROS:
					return new Timestamp(group.getLong(index, 0)/1000L);
				case NANOS:
					return new Timestamp(group.getLong(index, 0)/1000000L);
				default:
					return null;
				}
			} else if (otype instanceof JsonLogicalTypeAnnotation) {
				/*
				 * Json is returned as a json string
				 */
				return group.getString(index, 0);
			} else if (otype instanceof UUIDLogicalTypeAnnotation) {
				byte[] uuid = group.getBinary(index, 0).getBytes();
				return UUID.nameUUIDFromBytes(uuid).toString();
			}
			return null;
		}
		
	}
	
}