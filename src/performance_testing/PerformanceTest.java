package performance_testing;

import app_kvECS.ECSClient;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static java.lang.Thread.sleep;

public class PerformanceTest extends TestCase {

  private int[] clientHostList = { };
  private final int NUM_PAIRS = 1000;
  private String zookeeperIP;

//  private String[] cacheStragegies = { "FIFO", "LRU", "LFU" };
//  private String[] cacheStragegies = { "FIFO" };
//  private String[] cacheStragegies = { "LRU" };
  private String[] cacheStragegies = { "LFU" };
  private int[] numberOfClients = { 10 };
  private int[] numberOfServers = { 25 };
  private ECSClient ecsClient;

  @Before
  public void setUp() {
    try {
      zookeeperIP = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  private void sshStartClient(int numPairs, String remoteMachine, String outFile) {
    Process proc;
    String script = "scripts/ssh-start-perf-client.sh";
    String[] cmdArray = { script,
                          remoteMachine,
                          String.valueOf(numPairs),
                          outFile};
    Runtime run = Runtime.getRuntime();
    try {
      proc = run.exec(cmdArray);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private long getAvgTime(String cacheStrategy) {
    long sum = 0;
    File dataDir = new File("perf_out/" + cacheStrategy);
    File[] dataFileNames = dataDir.listFiles();
    if (dataFileNames == null) {
      System.out.println("dataFileNames was null!");
    }
    for (File dataFile : dataFileNames) {
      try {
        FileReader fr = new FileReader(dataFile.getAbsoluteFile());
        BufferedReader br = new BufferedReader(fr);
        sum += Long.parseLong(br.readLine().trim());
        br.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return sum / dataFileNames.length;
  }

  private void deleteFolderContents(String cacheStrategy) {
    File dir = new File("perf_out/" + cacheStrategy);
    File[] dataFiles = dir.listFiles();
    if (dataFiles != null) {
      for (File f : dataFiles) {
        f.delete();
      }
    }
  }

  private void clearStorage() {
    Runtime rt = Runtime.getRuntime();
    try {
      Process pr = rt.exec("rm -rf ~/KVServer_Storage/ && rm -rf ~/logs");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void doPerformanceTest(PrintWriter datFileWriter, int cacheSize) {
    for (int numServers : numberOfServers) {
      datFileWriter.print(numServers + "-SERVER\t");
      for (String cacheStrat : cacheStragegies) {
        for (int numClients : numberOfClients) {
          deleteFolderContents(cacheStrat);
          clearStorage();
          ecsClient = new ECSClient("ecs-perf.config", zookeeperIP, 1920);
          try {
            sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          ecsClient.addNodes(numServers, cacheStrat, cacheSize);
          try {
            // Give the servers a second to start up and set their watches.
            sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          ecsClient.start();
          int startClientMachine = 224;
          int endClientMachine = startClientMachine + numClients;
          for (int i = startClientMachine; i < endClientMachine; i++) {
            sshStartClient(NUM_PAIRS, "ug"+i, "perf_out/" + cacheStrat  + "/client-ug" + i);
          }
          int resultsReady = 0;
          File dir = new File("perf_out/" + cacheStrat);
          int numToWaitFor = (numClients == 1 ? 1 : numClients - 1); // One of the ug machines down
          while (resultsReady < numToWaitFor) {
            // Wait for enough clients to collect their data
            resultsReady = dir.list().length;
          }
          try {
            sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          System.out.println("doing getAvgTime for cache: " + cacheStrat + " numClients: " + numClients);
          long avgTime = getAvgTime(cacheStrat);
          deleteFolderContents(cacheStrat);
          datFileWriter.print(avgTime + "\t");
          datFileWriter.flush();
          ecsClient.shutdown();
          ecsClient.kill();
        }
      }
      datFileWriter.println();
      datFileWriter.flush();
    }
    datFileWriter.close();
  }

  private PrintWriter datFileSetup(int cacheSize) {
    File datFile = new File("perf_out/cache_size_" + cacheSize + ".dat");
    try {
      PrintWriter writer = new PrintWriter(datFile);
      // File Header:
      writer.println("#Test run data with cache size " + cacheSize + " and " + NUM_PAIRS + " puts/gets.");
      writer.println("#");
      // Column Labels:
      writer.print("TestRun\t");
      for (String cacheStrat : cacheStragegies) {
        for (int numClients : numberOfClients) {
          writer.print(cacheStrat + "-" + numClients + "-CLIENTS\t");
        }
      }
      writer.println();
      writer.flush();
      return writer;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Test
  public void testPerformanceCacheSize50() {
    PrintWriter datFile = datFileSetup(50);
    doPerformanceTest(datFile, 50);
    assertEquals(1, 1);
  }

//  @Test
//  public void testPerformanceCacheSize50() {
//    PrintWriter datFile = datFileSetup(50);
//    doPerformanceTest(datFile, 50);
//    assertEquals(1, 1);
//  }
//  @Test
//  public void testPerformanceCacheSize100() {
//    PrintWriter datFile = datFileSetup(100);
//    doPerformanceTest(datFile, 100);
//    assertEquals(1, 1);
//  }

}
