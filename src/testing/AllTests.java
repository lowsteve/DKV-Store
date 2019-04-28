package testing;

import app_kvECS.ECSClient;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    ConnectionTest.class,
    InteractionTest.class,
    HashRingTest.class,
    AdditionalTest.class,
    ECSTests.class
})

public class AllTests {
  static ECSClient ecsClient;

  @BeforeClass
  public static void setUpClass() {
    try {
      new LogSetup("logs/testing/test.log", Level.INFO);
      ecsClient = new ECSClient("ecs.config", "localhost", 1920);
      ecsClient.addNodes(3, "FIFO", 50);
      try {
        // For some reason we need to give the servers extra time to start up.
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      ecsClient.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDownClass() {
    ecsClient.shutdown();
    ecsClient.quit();
  }

//  public static Test suite() {
//    TestSuite clientSuite = new TestSuite("Distributed Server Test-Suite");
//    clientSuite.addTestSuite(ConnectionTest.class);
//    clientSuite.addTestSuite(InteractionTest.class);
//    clientSuite.addTestSuite(AdditionalTest.class);
//    return clientSuite;
//  }
}
