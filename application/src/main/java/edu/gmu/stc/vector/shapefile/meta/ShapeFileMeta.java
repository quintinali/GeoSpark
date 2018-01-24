package edu.gmu.stc.vector.shapefile.meta;

import java.io.Serializable;

/**
 * Created by Fei Hu.
 */
public class ShapeFileMeta implements Serializable {
  private ShpMeta shpMeta = null;
  private DbfMeta dbfMeta = null;

  private String shpFilePath = null;
  private String dbfFilePath = null;

  public ShapeFileMeta(ShpMeta shpMeta, DbfMeta dbfMeta, String shpFilePath, String dbfFilePath) {
    this.shpMeta = shpMeta;
    this.dbfMeta = dbfMeta;
    this.shpFilePath = shpFilePath;
    this.dbfFilePath = dbfFilePath;
  }

}
