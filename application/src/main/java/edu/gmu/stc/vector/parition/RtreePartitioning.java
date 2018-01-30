/**
 * FILE: RtreePartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.RtreePartitioning.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package edu.gmu.stc.vector.parition;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.strtree.STRtree;

import org.apache.commons.io.FileUtils;
import org.wololo.jts2geojson.GeoJSONWriter;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;

// TODO: Auto-generated Javadoc

/**
 * The Class RtreePartitioning.
 */
public class RtreePartitioning implements Serializable{

	/** The grids. */
	final List<Envelope> grids = new ArrayList<>();

        /**
         * Instantiates a new rtree partitioning.
         *
         * @param samples the sample list
         * @param partitions the partitions
         * @throws Exception the exception
         */
        public RtreePartitioning(List<ShapeFileMeta> samples, int partitions) throws Exception
        {
          STRtree strtree = new STRtree(samples.size()/partitions);
          for (ShapeFileMeta sample : samples) {
            strtree.insert(sample.getEnvelopeInternal(), sample);
          }

          List<Envelope> envelopes = strtree.queryBoundary();
          for (Envelope envelope : envelopes) {
            grids.add(envelope);
          }

          //saveGridAsGeoJSON("/Users/feihu/Documents/GitHub/GeoSpark/" + samples.size() + ".geojson");
        }

	/**
	 * Gets the grids.
	 *
	 * @return the grids
	 */
	public List<Envelope> getGrids() {
		
		return this.grids;
		
	}

	public void saveGridAsGeoJSON(String filePath) throws IOException {
          GeoJSONWriter writer = new GeoJSONWriter();
          List<Feature> featureList = new ArrayList<Feature>();
          GeometryFactory geometryFactory = new GeometryFactory();
          for (Envelope envelope : grids) {
            featureList.add(new Feature(writer.write(geometryFactory.toGeometry(envelope)), null));
          }

          FeatureCollection featureCollection = new FeatureCollection(featureList.toArray(new Feature[featureList.size()]));
          FileUtils.writeStringToFile(new File(filePath), featureCollection.toString());
        }
}
