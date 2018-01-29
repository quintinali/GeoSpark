package edu.gmu.stc.vector.parition;

import com.vividsolutions.jts.geom.Envelope;

import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialPartitioning.EqualPartitioning;
import org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning;
import org.datasyslab.geospark.spatialPartitioning.KDBTree;
import org.datasyslab.geospark.spatialPartitioning.KDBTreePartitioner;
import org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning;
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadTreePartitioner;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;

import java.util.Collection;
import java.util.List;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;

import static org.datasyslab.geospark.enums.GridType.EQUALGRID;
import static org.datasyslab.geospark.enums.GridType.HILBERT;
import static org.datasyslab.geospark.enums.GridType.KDBTREE;
import static org.datasyslab.geospark.enums.GridType.QUADTREE;

/**
 * Created by Fei Hu on 1/26/18.
 */
public class PartitionUtil {

  public static SpatialPartitioner spatialPartitioning(GridType gridType, int numPartitions,
                                                       List<ShapeFileMeta> samples)
      throws Exception {
    List<Envelope> grids;
    SpatialPartitioner partitioner;
    StandardQuadTree partitionTree;

    // Add some padding at the top and right of the boundaryEnvelope to make
    // sure all geometries lie within the half-open rectangle.
    // TODO: Is the padding value reasonable here? The ratio number may be more reasonable.

    Envelope paddedBoundary = null;

    if (gridType == EQUALGRID || gridType == HILBERT || gridType == QUADTREE || gridType == KDBTREE) {

      double minX = Double.MAX_VALUE, minY = Double.MIN_VALUE,
          maxX = Double.MIN_VALUE, maxY = Double.MIN_VALUE;

      for (ShapeFileMeta shapeFileMeta : samples) {
        if (minX > shapeFileMeta.getMinX()) minX = shapeFileMeta.getMinX();
        if (minY > shapeFileMeta.getMinY()) minY = shapeFileMeta.getMinY();
        if (maxX < shapeFileMeta.getMaxX()) maxX = shapeFileMeta.getMaxX();
        if (maxY < shapeFileMeta.getMaxY()) maxY = shapeFileMeta.getMaxY();
      }

      paddedBoundary = new Envelope(minX, maxX + 0.01, minY, maxY + 0.01);
    }

    switch(gridType) {
      case RTREE: {
        RtreePartitioning
            rtreePartitioning = new RtreePartitioning(samples, numPartitions);
        grids = rtreePartitioning.getGrids();
        partitioner = new FlatGridPartitioner(grids);
        break;
      }

      default:
        throw new Exception("[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method.");
    }

    return partitioner;

  }

}
