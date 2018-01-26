package edu.gmu.stc.vector.shapefile.meta;

/**
 * Created by Fei Hu on 1/25/18.
 */
public class TestShapeFileMeta {

  public static void main(String[] args) {
    System.out.println(ShapeFileMeta.getSQLForOverlappedRows("shapeFile",
                                                             -77.0411709654385, 38.9956105073279,
                                                             -77.0413824515586, 38.9954578167531));
  }

}
