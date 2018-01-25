package edu.gmu.stc.hibernate;

import org.hibernate.Session;

import javax.print.Doc;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;

/**
 * Created by Fei Hu on 1/25/18.
 */
public class TestHibernate {

  public static void main(String[] args) {
    String tableName = "table_shp";
    PhysicalNameStrategyImpl physicalNameStrategy = new PhysicalNameStrategyImpl(tableName);
    Session session = HibernateUtil
        .createSessionFactoryWithPhysicalNamingStrategy(physicalNameStrategy, ShapeFileMeta.class)
        .openSession();
    DAOImpl dao = new DAOImpl();
    dao.setSession(session);
    ShapeFileMeta shapeFileMeta = new ShapeFileMeta(31l, 5, 10l, 10,
                                                    20l, 20, "a/b/c",
                                                    -0.5, -0.5, -0.5, -0.5);
    dao.insertDynamicTableObject(tableName, shapeFileMeta);
    session.close();

    HibernateUtil.shutdown();

  }

}
