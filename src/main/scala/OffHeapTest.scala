package com.globalmaksimum.ignite

import java.io.{InputStream, OutputStream}
import java.lang.management.ManagementFactory

import org.apache.ignite.Ignition
import org.apache.ignite.cache.eviction.lru.CacheLruEvictionPolicy
import org.apache.ignite.cache.{CacheAtomicityMode, CacheMemoryMode, CacheMode}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.marshaller.AbstractMarshaller
import org.nustaq.serialization.FSTConfiguration

import scala.util.Random

/**
 * Created by capacman on 2/25/15.
 */

class FSTMarshaller(conf: FSTConfiguration) extends AbstractMarshaller {
  override def marshal(obj: scala.Any, out: OutputStream): Unit = {
    val objout = conf.getObjectOutput(out)
    objout.writeObject(obj, null)
    objout.flush()
  }

  override def unmarshal[T](in: InputStream, clsLdr: ClassLoader): T = {
    val objin = conf.getObjectInput(in)
    val result = objin.readObject()
    result.asInstanceOf[T]
  }
}

case class Person(name: String, surname: String)

object OffHeapTest extends App {

  val cacheCfg = new CacheConfiguration()

  cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED)
  cacheCfg.setCacheMode(CacheMode.LOCAL)
  cacheCfg.setEvictionPolicy(new CacheLruEvictionPolicy[String, Person](200000))
  cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC)
  cacheCfg.setManagementEnabled(true)
  cacheCfg.setStatisticsEnabled(true)

  // Set off-heap memory to 10GB (0 for unlimited)
  cacheCfg.setOffHeapMaxMemory(10 * 1024L * 1024L * 1024L)


  val cacheName: String = "myOffHeapCache"
  cacheCfg.setName(cacheName)

  val cfg = new IgniteConfiguration()


  cfg.setLocalHost("127.0.0.1")

  cfg.setCacheConfiguration(cacheCfg)
  cfg.setMBeanServer(ManagementFactory.getPlatformMBeanServer)

  val fstConf = FSTConfiguration.createDefaultConfiguration()
  fstConf.registerClass(classOf[Person])
  cfg.setMarshaller(new FSTMarshaller(fstConf))
  // Start Ignite node.
  val ignite = Ignition.start(cfg)


  val jcache = ignite.jcache[String, Person](cacheName)
  val loader = ignite.dataLoader[String, Person](cacheName)
  loader.perNodeBufferSize(100000)


  0 until 1000000 foreach {
    i =>
      if (i % 100000 == 0)
        println(s"current size is $i")
      jcache.put(s"k%10d".format(i), Person(s"n%10d".format(i), s"s%10d".format(i)))
  }
  loader.close()
  println(s"cache size is ${jcache.size()}")


  0 until 200000 foreach {
    i =>
      val value = jcache.get(s"k%10d".format(i))
  }

  val start = System.currentTimeMillis()
  val r = new Random()
  0 until 1000000 foreach {
    i =>
      val value = jcache.get(s"k%10d".format(r.nextInt(440000)))
  }
  println(jcache.get(s"k%10d".format(r.nextInt(440000))))
  println(s"total time for lookup ${System.currentTimeMillis() - start}")
  println(jcache.metrics())
}

