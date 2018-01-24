package edu.gmu.stc.vector.shapefile.meta;

import org.datasyslab.geospark.formatMapper.shapefileParser.boundary.BoundBox;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeType;

/**
 * Created by Fei Hu on 1/24/18.
 */
public class ShpMeta {
  private Long index;
  private int typeID;
  private int offset;
  private int length;
  private BoundBox boundBox;

  public ShpMeta(Long index, int typeID, int offset, int length, BoundBox boundBox) {
    this.index = index;
    this.typeID = typeID;
    this.offset = offset;
    this.length = length;
    this.boundBox = boundBox;
  }
}
