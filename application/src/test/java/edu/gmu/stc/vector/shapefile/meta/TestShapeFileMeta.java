package edu.gmu.stc.vector.shapefile.meta;

/**
 * Created by Fei Hu on 1/25/18.
 */
public class TestShapeFileMeta {

  public static void main(String[] args) {
    System.out.println(ShapeFileMeta.getSQLForOverlappedRows("shapeFile",
                                                             -100.0, -100.0,
                                                             100.0, 100.0));
  }

}
