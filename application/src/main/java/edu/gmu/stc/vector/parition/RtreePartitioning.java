/**
 * FILE: RtreePartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.RtreePartitioning.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package edu.gmu.stc.vector.parition;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

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
        }

	/**
	 * Gets the grids.
	 *
	 * @return the grids
	 */
	public List<Envelope> getGrids() {
		
		return this.grids;
		
	}
}
