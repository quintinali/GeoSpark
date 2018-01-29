package edu.gmu.stc.hibernate;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Environment;
import org.hibernate.jpa.event.internal.jpa.EntityCallback;

import edu.gmu.stc.config.ConfigParameter;
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;
import edu.gmu.stc.vector.shapefile.meta.ShpMeta;

/**
 * Created by Fei Hu on 1/25/18.
 */
public class HibernateUtil {

  private static StandardServiceRegistry registry;
  private static SessionFactory sessionFactory;

  public static SessionFactory getSessionFactory(Configuration conf) {
    if (sessionFactory == null) {
      try {

        // Create registry builder
        StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder();

        // Hibernate settings equivalent to hibernate.cfg.xml's properties
        Map<String, String> settings = new HashMap<>();
        settings.put(Environment.DRIVER, conf.get(ConfigParameter.HIBERNATE_DRIEVER));
        settings.put(Environment.URL, conf.get(ConfigParameter.HIBERNATE_URL));
        settings.put(Environment.USER, conf.get(ConfigParameter.HIBERNATE_USER));
        settings.put(Environment.PASS, conf.get(ConfigParameter.HIBERNATE_PASS));
        settings.put(Environment.DIALECT, conf.get(ConfigParameter.HIBERNATE_DIALECT));
        settings.put(Environment.HBM2DDL_AUTO, conf.get(ConfigParameter.HIBERNATE_HBM2DDL_AUTO));

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

  public static <T> SessionFactory createSessionFactoryWithPhysicalNamingStrategy(Configuration conf,
      PhysicalNameStrategyImpl physicalNameStrategy,
      Class<T> mappingClass) {
    // Create registry builder
    StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder();

    // Hibernate settings equivalent to hibernate.cfg.xml's properties
    Map<String, String> settings = new HashMap<>();
    settings.put(Environment.DRIVER, conf.get(ConfigParameter.HIBERNATE_DRIEVER));
    settings.put(Environment.URL, conf.get(ConfigParameter.HIBERNATE_URL));
    settings.put(Environment.USER, conf.get(ConfigParameter.HIBERNATE_USER));
    settings.put(Environment.PASS, conf.get(ConfigParameter.HIBERNATE_PASS));
    settings.put(Environment.DIALECT, conf.get(ConfigParameter.HIBERNATE_DIALECT));
    settings.put(Environment.HBM2DDL_AUTO, conf.get(ConfigParameter.HIBERNATE_HBM2DDL_AUTO));

    // Apply settings
    registryBuilder.applySettings(settings);

    // Create registry
    registry = registryBuilder.build();

    try {
      MetadataSources sources = new MetadataSources(registry);
      sources.addAnnotatedClass(mappingClass);
      //sources.addClass(mappingClass);
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
