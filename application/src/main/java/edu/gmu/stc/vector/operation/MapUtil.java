package edu.gmu.stc.vector.operation;

import edu.gmu.stc.config.ConfigParameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Yun Li on 2/28/18.
 */
public class MapUtil {

	public static String localToRemote(String localFile, String hostname, String username, String password,
			String remoteDir) {
		String remotePath = "";
		SSHCommander commander = new SSHCommander(hostname, username, password);
		try {
			remotePath = commander.copyDirToRemote(localFile, remoteDir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		commander.closeConnection();

		return remotePath;
	}
	
	public static Map<String, String> getShpInfo(String filepath){

		Map<String, String> mapInfo = new HashMap<>();

		File file = new File(filepath);
		if(!file.exists() || !filepath.endsWith(".shp")) {
			try {
				throw new Exception("Invalid shapefile filepath: " + filepath);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		ShapefileDataStore dataStore = null;
		try {
			dataStore = new ShapefileDataStore(file.toURL());
			ContentFeatureSource featureSource = dataStore.getFeatureSource();

			// get dynamically the CRS of your data:
			SimpleFeatureType schema = featureSource.getSchema();
			CoordinateReferenceSystem sourceCRS = schema.getCoordinateReferenceSystem();
			String code = CRS.lookupIdentifier( sourceCRS, true );
			mapInfo.put("code", code);

			//bbox=984018.1663741902,207673.09513056703,991906.4970533887,219622.53973435296
			ReferencedEnvelope env = featureSource.getBounds();
			String bbox = env.getMinimum(0) + "," + env.getMinimum(1) + "," + env.getMaximum(0) + "," + env.getMaximum(1);
			mapInfo.put("bbox", bbox);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return mapInfo;
	}

	public static String publishWMS(String shpFolder, String cfgPath){
		//load configaration
		Configuration hConf = new Configuration();
		hConf.addResource(new Path(cfgPath));

		File curDir = new File(shpFolder);
		String[] files = curDir.list();
		String shpFileName = "";
		String shpFilePath = "";
		for(String file: files){
			if(file.endsWith(".shp")){
				shpFileName = file;
				shpFilePath = shpFolder + "/" + shpFileName;
			}
		}

		//get projection and boundary box
		Map<String, String> mapInfo = MapUtil.getShpInfo(shpFilePath);
		String code = mapInfo.get("code");
		String bbox = mapInfo.get("bbox");
		System.out.println("espg code:" + code);
		System.out.println("bbox:" + bbox);

		//copy folder to remote machine
		String remoteDir = hConf.get(ConfigParameter.GEOSERVER_DATA_DIRECTORY);
		String hostname = hConf.get(ConfigParameter.GEOSERVER_HOST_IP);
		String username = hConf.get(ConfigParameter.GEOSERVER_HOST_USER);
		String password = hConf.get(ConfigParameter.GEOSERVER_HOST_PWD);
		String remotePath = MapUtil.localToRemote(shpFolder, hostname, username, password,  remoteDir);
		System.out.println("copy local shp file to :" + hostname + ":" + remotePath);

		//publish map
		String restURL = hConf.get(ConfigParameter.GEOSERVER_RESTURL);
		String restUser = hConf.get(ConfigParameter.GEOSERVER_RESTUSER);
		String restPWD = hConf.get(ConfigParameter.GEOSERVER_RESTPWD);
		Geoclient client = new Geoclient(restURL, restUser, restPWD);
		String workspace = "mapWorkspace";
		String dataStore = shpFileName + Math.random();
		remotePath = "file:///" + remotePath + shpFileName;
		String datasetName = shpFileName.replace(".shp", "");
		String publishedUrl = client.publishShapefile(workspace, dataStore, datasetName, remotePath, code, bbox);
		System.out.println("publish file: " + workspace + "| " + dataStore +  "| " +  datasetName + "| " + remotePath);

		return publishedUrl;
	}

	public static void main( String[] args ) throws IOException
	{
		MapUtil.publishWMS("D:\\python workspace\\data\\Soil_Type_by_Slope\\", "D:\\geosparkWorkspace\\GeoSpark\\config\\config.xml");
	}
}
