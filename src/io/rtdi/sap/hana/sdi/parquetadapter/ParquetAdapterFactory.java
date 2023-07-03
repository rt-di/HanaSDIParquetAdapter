package io.rtdi.sap.hana.sdi.parquetadapter;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class ParquetAdapterFactory implements AdapterFactory{

	public static final String CREDENTIAL = "credential";
	public static final String ROOTURL = "rooturl";
	public static final String CONFDIR = "confdir";

	@Override
	public Adapter createAdapterInstance() {
		return new ParquetAdapter();
	}

	@Override
	public String getAdapterType() {
		return "ParquetAdapter";
	}

	@Override
	public String getAdapterDisplayName() {
		return "ParquetAdapter";
	}

	@Override
	public String getAdapterDescription() {
		return "SDI Adapter ParquetAdapter";
	}
	
	@Override
	public RemoteSourceDescription getAdapterConfig() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		
		PropertyGroup connectionInfo = new PropertyGroup("connectioninfo","hdfs Parameters","hdfs Parameters");
		PropertyEntry hdfsurl = new PropertyEntry(ROOTURL, "hdfs root directory", "The URL of the root directory in hdfs format");
		hdfsurl.setDefaultValue("file:///data");
		connectionInfo.addProperty(hdfsurl);
		
		PropertyEntry confdir = new PropertyEntry(CONFDIR, "directory with hadoop configuration files", "Th directory where all hadoop client configuration files are found");
		confdir.setDefaultValue("file:///hadoop/conf");
		connectionInfo.addProperty(confdir);

		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry(CREDENTIAL, "Arbitary Adapter Credentials the remote source must know");
		credential.getUser().setDisplayName("Username");
		credential.getPassword().setDisplayName("Password");
		credentialProperties.addCredentialEntry(credential);

		rs.setCredentialProperties(credentialProperties);
		rs.setConnectionProperties(connectionInfo);
		return rs;
	}
	
	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription remoteSourceDescription) throws AdapterException {
		return true;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription propertyGroup) throws AdapterException {
		return null;
	}
}
