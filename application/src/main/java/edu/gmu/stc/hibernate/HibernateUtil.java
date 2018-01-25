package edu.gmu.stc.hibernate;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Environment;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;
import edu.gmu.stc.vector.shapefile.meta.ShpMeta;

/**
 * Created by Fei Hu on 1/25/18.
 */
public class HibernateUtil {

  private static StandardServiceRegistry registry;
  private static SessionFactory sessionFactory;

  public static SessionFactory getSessionFactory() {
    if (sessionFactory == null) {
      try {

        // Create registry builder
        StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder();

        // Hibernate settings equivalent to hibernate.cfg.xml's properties
        Map<String, String> settings = new HashMap<>();
        settings.put(Environment.DRIVER, "org.postgresql.Driver");
        settings.put(Environment.URL, "jdbc:postgresql://localhost:5432/hibernate_test");
        settings.put(Environment.USER, "feihu");
        settings.put(Environment.PASS, "feihu");
        settings.put(Environment.DIALECT, "org.hibernate.dialect.PostgreSQL9Dialect");
        settings.put(Environment.HBM2DDL_AUTO, "create");

        // Apply settings
        registryBuilder.applySettings(settings);

        // Create registry
        registry = registryBuilder.build();

        // Create MetadataSources
        MetadataSources sources = new MetadataSources(registry);

        sources.addAnnotatedClass(ShapeFileMeta.class);

        // Create Metadata
        Metadata metadata = sources.getMetadataBuilder().build();

        // Create SessionFactory
        sessionFactory = metadata.getSessionFactoryBuilder().build();


      } catch (Exception e) {
        e.printStackTrace();
        if (registry != null) {
          StandardServiceRegistryBuilder.destroy(registry);
        }
      }
    }
    return sessionFactory;
  }

  public static <T> SessionFactory createSessionFactoryWithPhysicalNamingStrategy(
      PhysicalNameStrategyImpl physicalNameStrategy,
      Class<T> mappingClass) {
    // Create registry builder
    StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder();

    // Hibernate settings equivalent to hibernate.cfg.xml's properties
    Map<String, String> settings = new HashMap<>();
    settings.put(Environment.DRIVER, "org.postgresql.Driver");
    settings.put(Environment.URL, "jdbc:postgresql://localhost:5432/hibernate_test");
    settings.put(Environment.USER, "feihu");
    settings.put(Environment.PASS, "feihu");
    settings.put(Environment.DIALECT, "org.hibernate.dialect.PostgreSQL9Dialect");
    settings.put(Environment.HBM2DDL_AUTO, "update");

    // Apply settings
    registryBuilder.applySettings(settings);

    // Create registry
    registry = registryBuilder.build();

    try {
      MetadataSources sources = new MetadataSources(registry);
      sources.addAnnotatedClass(mappingClass);
      MetadataBuilder metadataBuilder = sources.getMetadataBuilder();

      metadataBuilder.applyPhysicalNamingStrategy(physicalNameStrategy);
      sessionFactory = metadataBuilder.build().buildSessionFactory();

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Hibernate session factory setup error: " + e);
      StandardServiceRegistryBuilder.destroy(registry);
    }

    return sessionFactory;
  }

  public static void shutdown() {
    if (registry != null) {
      StandardServiceRegistryBuilder.destroy(registry);
    }
  }

}
