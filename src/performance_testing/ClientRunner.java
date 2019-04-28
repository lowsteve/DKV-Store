package performance_testing;

import client.KVStore;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ClientRunner{
  private final String ENRON_DIR = "enron_data";

  private KVStore kvStore;
  private List<String> keys;
  private List<String> values;

  public ClientRunner() {
    this.kvStore = new KVStore("ug132", 21231);
    this.keys = new ArrayList<>();
    this.values = new ArrayList<>();
  }

  private void writeTimeToFile(long runningTime, String outFileName) {
    try {
      File file = new File(outFileName);
      if (!file.exists()) {
        file.createNewFile();
      }
//      FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(runningTime + "\n");
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void timePutsGets(String outFileName) {
    try {
      this.kvStore.connect();
    } catch (Exception e) {
      e.printStackTrace();
    }
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < keys.size(); i++) {
      try {
        System.out.println("Doing put #" + i);
        kvStore.put(keys.get(i), values.get(i));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    for (int i = 0; i < keys.size(); i++) {
      try {
        System.out.println("Doing get #" + i);
        kvStore.get(keys.get(i));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    long runTime = System.currentTimeMillis() - startTime;
    writeTimeToFile(runTime, outFileName);
    kvStore.disconnect();

  }

  public void loadData(int numPairs) {
    int loadedCount = 0;
    File enronDir = new File(ENRON_DIR);
    String[] directories = enronDir.list();
    assert(directories != null);
    for (String dir : directories) {
      File inboxDir = new File(ENRON_DIR + "/" + dir + "/inbox");
      String[] inboxFiles = inboxDir.list();
      if (inboxFiles == null) {
        continue;
      }
      for (String filename : inboxFiles) {
        try {
          InputStream is = new FileInputStream(ENRON_DIR + "/" + dir + "/inbox/" + filename);
          BufferedReader buf = new BufferedReader(new InputStreamReader(is));
          String key = dir + ":" + filename;
          int maxLen = (key.length() < 20) ? key.length() : 20;
          keys.add(key.substring(0, maxLen));
          values.add(buf.readLine());
          buf.close();
          is.close();
          loadedCount++;
          if (loadedCount >= numPairs) {
            break;
          }
        } catch (FileNotFoundException e) {
          System.out.println("loadData: FileNotFoundException");
          e.printStackTrace();
        } catch (IOException e) {
          System.out.println("loadData: IOException");
          e.printStackTrace();
        }
      }
      if (loadedCount >= numPairs) {
        break;
      }
    }
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Error! Invalid number of arguments.");
      System.out.println("\tUsage: java -jar perf-client-runner.jar <number-of-pairs> <out-file-name>");
      System.exit(1);
    }
    ClientRunner clientRunner = new ClientRunner();
    clientRunner.loadData(Integer.parseInt(args[0]));
    clientRunner.timePutsGets(args[1]);
  }
}
