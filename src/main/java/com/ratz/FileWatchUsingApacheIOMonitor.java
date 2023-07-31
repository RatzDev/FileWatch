package com.ratz;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;


public class FileWatchUsingApacheIOMonitor {

    public static void main(String[] args) throws Exception {
        String folderPath = "./inputdir"; // Replace with the actual folder path
        // Create a WatchService
        Path folder = Paths.get(folderPath);

        long pollingInterval = 10000; // Polling interval in milliseconds (adjust as needed)

        // Create a new FileAlterationObserver for the specified folder
        System.out.println("Listening for new files...>>>>>>" + folder.toFile().getAbsolutePath());
        FileAlterationObserver observer = new FileAlterationObserver(folder.toFile());

        // Add a listener to the observer to handle file system events
        observer.addListener(new CustomFileListener());

        // Create a new FileAlterationMonitor with the specified polling interval
        FileAlterationMonitor monitor = new FileAlterationMonitor(pollingInterval);

        // Add the observer to the monitor
        monitor.addObserver(observer);

        // Start the monitor to begin listening for file system changes
        monitor.start();



    }


    static class CustomFileListener extends FileAlterationListenerAdaptor {
        private static boolean initialRun = true;
        @Override
        public void onStart(FileAlterationObserver observer) {
          super.onStart(observer);
          if(initialRun) {
              initialRun=false;
              System.out.println("Listener started for observing files in the directory...>>>>>>" + observer.getDirectory().getAbsolutePath());
              File[] files = observer.getDirectory().listFiles();
              if (files != null) {
                  for (File file : files) {
                      if (file.isFile()) {
                          readFileWithReadLock(file);
                      }
                  }
              }
          }

        }

        @Override
        public void onFileCreate(File file) {
            System.out.println("New file detected: " + file.getAbsolutePath());
//            readFileWithReadLock(file);
            readFileWithReadLock(file);
            // Add your logic to process the new file here
            // For example, you can call a method to read and process the file's content.
        }


        @Override
        public void onFileDelete(File file) {
            // code for processing deletion event
            System.out.println("File deleted: " + file.getAbsolutePath());

        }

        @Override
        public void onFileChange(File file) {
            System.out.println("File modified: " + file.getAbsolutePath());

        }
    }


    private static void readFileWithReadLock(File file) {
        String threadName = Thread.currentThread().getName();
        System.out.println("Thread Name: " + threadName);
        boolean locked = true;
        int timeoutInSeconds = 60* 10;
        RandomAccessFile raf = null;

        long startTime = System.currentTimeMillis();
        while (locked) {
            if (isTimeoutExceeded(startTime, timeoutInSeconds)) {
                System.out.println("Cannot acquire read lock within " + timeoutInSeconds + " seconds. Will skip the file: " + file.getAbsolutePath());
                break;
            }


            try {
                raf = new RandomAccessFile(file, "r"); // it will throw FileNotFoundException. It's not needed to use 'rw' because if the file is delete while copying, 'w' option will create an empty file.
                readFile(raf);
                // Delete the file after reading
                if (file.delete()) {
                    System.out.println("File deleted: " + file.getAbsolutePath());
                } else {
                    System.out.println("Failed to delete file: " + file.getAbsolutePath());
                }
                locked = false;
            } catch (IOException e) {
                locked = file.exists();
                if (locked) {
                    System.out.println("File locked: '" + file.getAbsolutePath() + "'");
                    try {
                        Thread.sleep(5000); // waits some time
                    } catch (InterruptedException ex) {
                        e.printStackTrace(); // replace with logging
                    }
                } else {
                    System.out.println("File was deleted while copying: '" + file.getAbsolutePath() + "'");
                    break;
                }
            } finally {

                if (raf != null) {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        e.printStackTrace(); // replace with logging
                    }
                }
            }

        }
    }


    private static void readFile(RandomAccessFile file) {
        // Read the file content here
        System.out.println("Reading new file...>>>>>>");
        try (BufferedReader reader = new BufferedReader(new FileReader(file.getFD()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Process each line in the file
//                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace(); // replace with logging
        }
    }


    private static boolean isTimeoutExceeded(long startTime, int timeoutInSeconds) {
        long currentTime = System.currentTimeMillis();
        long elapsedTimeInSeconds = (currentTime - startTime) / 1000;
        return elapsedTimeInSeconds >= timeoutInSeconds;
    }


    private static String getFileHash(File file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(Files.readAllBytes(Paths.get(file.getAbsolutePath())));
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();  // replace with logging
            return null;
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

}
