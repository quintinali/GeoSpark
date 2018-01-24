package edu.gmu.stc.vector.shapefile.meta;

import java.io.Serializable;

/**
 * Created by Fei Hu on 1/24/18.
 */
public class DbfMeta implements Serializable{
  private Long index;
  private String value;

  public DbfMeta(Long index, String value) {
    this.index = index;
    this.value = value;
  }

}
