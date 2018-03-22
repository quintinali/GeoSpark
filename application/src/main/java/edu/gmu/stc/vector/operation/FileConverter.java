package edu.gmu.stc.vector.operation;

import java.io.File;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.util.json.JSONException;
import com.vividsolutions.jts.geom.*;
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONObject;
import org.opengis.referencing.FactoryException;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileConverter {
    /**
     * convert geojson to shp
     * @param jsonPath
     * @param shpPath
     * @return
     */
    public static Map geojson2Shape(String jsonPath, String shpPath){
        Map map = new HashMap();
        GeometryJSON gjson = new GeometryJSON();
        try{
            //create shape file
            File file = new File(shpPath);
            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put( ShapefileDataStoreFactory.URLP.key, file.toURI().toURL() );
            ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

            //define attribute
            SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
            tb.setCRS(DefaultGeographicCRS.WGS84);
            tb.setName("shapefile");
            tb.add("the_geom", Polygon.class);
            tb.add("POIID", Long.class);
            ds.createSchema(tb.buildFeatureType());
            //set code
            Charset charset = Charset.forName("GBK");
            ds.setCharset(charset);
            //set writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);

            File folder = new File(jsonPath);
            File[] listOfFiles = folder.listFiles();

            int id = 0;
            for (int j = 0; j < listOfFiles.length; j++) {
                if (listOfFiles[j].isFile()) {
                    String filePath = listOfFiles[j].getPath();
                    System.out.println(filePath);
                    String strJson = new String(Files.readAllBytes(Paths.get(filePath)));
                    JSONObject json = new JSONObject(strJson);
                    JSONArray features = (JSONArray) json.get("features");
                    JSONObject feature0 = new JSONObject(features.get(0).toString());
                    for (int i = 0, len = features.length(); i < len; i++) {
                        String strFeature = features.get(i).toString();
                        Reader reader = new StringReader(strFeature);
                        SimpleFeature feature = writer.next();
                        feature.setAttribute("the_geom", gjson.readPolygon(reader));
                        feature.setAttribute("POIID", id);

                        id += 1;
                        writer.write();
                    }
                }
            }

            System.out.println( id + " polygons");
            writer.close();
            ds.dispose();
            map.put("status", "success");
            map.put("message", shpPath);
        }
        catch(Exception e){
            map.put("status", "failure");
            map.put("message", e.getMessage());
            e.printStackTrace();
        }

        return map;
    }

    public static Map geometry2Shape(String jsonPath, String shpPath){
        Map map = new HashMap();
        GeometryJSON gjson = new GeometryJSON();
        List<Geometry> geometries = new ArrayList<Geometry>();
        try{
            File folder = new File(jsonPath);
            File[] listOfFiles = folder.listFiles();
            int id = 0;
            for (int j = 0; j < listOfFiles.length; j++) {
                if (listOfFiles[j].isFile()) {
                    String filePath = listOfFiles[j].getPath();
                    System.out.println(filePath);
                    String strJson = new String(Files.readAllBytes(Paths.get(filePath)));
                    JSONObject json = new JSONObject(strJson);
                    JSONArray features = (JSONArray) json.get("features");
                    //JSONObject feature0 = new JSONObject(features.get(0).toString());
                    for (int i = 0, len = features.length(); i < len; i++) {
                        String strFeature = features.get(i).toString();
                        Reader reader = new StringReader(strFeature);
                        Polygon polygon = gjson.readPolygon(reader);
                        geometries.add((Geometry) polygon);
                        id += 1;
                    }
                }
            }

            System.out.println(id + " polygon in total");
            GeometryReaderUtil.saveAsShapefile(shpPath, geometries, "epsg:4326");
            } catch (JSONException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (FactoryException e) {
            e.printStackTrace();
        }

        return map;
    }

    public static void main(String[] args){

        long start = System.currentTimeMillis();
        //String shpPath = "/Users/lzugis/Documents/chinadata/cityboundry.shp";
        //String jsonPath = "D:/Desktop/test031612.geojson/part-00000.json";
        //Map map = fileFormat.shape2Geojson(shpPath, jsonPath);

      String shpPath = "C:/Users/tinay/Desktop/shp032201.shp";
      String jsonPath = "C:/Users/tinay/Desktop/json032201/";
      Map map = FileConverter.geometry2Shape(jsonPath, shpPath);

        System.out.println(shpPath+", took "+(System.currentTimeMillis() - start)/1000 +"s");
    }
}