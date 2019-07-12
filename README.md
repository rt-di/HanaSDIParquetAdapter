# HanaSDIParquetAdapter
An Adapter for SAP Hana Smart Data Integration to read any Parquet file/directory located anywhere.

The adapter is based on the Hadoop client library, hence can read from local filesystem, HDFS, S3 or any other file system the Hadoop client supports.

It supports reading all parquet files, including nested structures. In the nested case the data is "joined" by simply selecting the addition columns.

Example: A Customer structure with 100 customer entries, each has three addresses
Customer
- FirstName (String)
- LastName (String)
- Address (Array)
  - City (String)
  - Country (String)
  - Type (String)

select FirstName, LastName will return 100 records.
select FirstName, LastName, Address[].City will return 300 records.


## Capabilities

- Select: Allows to select the table, no insert, update, delete
- Projection: Skip unneeded columns
- Top n: support select top 100... or select ... limit 100 statements
- simple where with AND on different columns: select * from table where LastName = 'Bauer' and Address[].Type = 'Home'
  - between: where LastName between 'A' and 'Z'
  - in-clause: where Address[].Type in ('Home', 'Office')
- simple aggregations

