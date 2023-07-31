package com.ratz;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;

public class FileWatch {

    public static void main(String[] args) throws IOException, InterruptedException {
        String folderPath = "/inputdir"; // Replace with the actual folder path

        // Create a WatchService
        Path folder = Paths.get(folderPath);

        WatchService watchService = folder.getFileSystem().newWatchService();

        folder.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);


        // Start listening for new files
        System.out.println("Listening for new files...>>>>>>" + folder.toFile().getAbsolutePath());
        while (true) {
            WatchKey key = watchService.take();
            System.out.println("Inside While Loop...>>>>>>");
            for (WatchEvent<?> event : key.pollEvents()) {
                System.out.println("Poll events loop...>>>>>>");
                WatchEvent.Kind<?> kind = event.kind();

                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    continue;
                }


                @SuppressWarnings("unchecked")
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path fileName = ev.context();
                Path filePath = folder.resolve(fileName);
                readFile(filePath);

                // Wait for file write completion
//                boolean isFileReady = waitUntilFileReady(filePath);
//                if (isFileReady) {
//                    // Read the file
//                    readFile(filePath);
//                }
            }

            key.reset();
        }
    }



    private static boolean waitUntilFileReady(Path filePath) throws InterruptedException {
        while (true) {
            try {
                // Attempt to open the file with exclusive access
                RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
                raf.close();
                return true;
            } catch (IOException e) {
                // File still being written, wait for a short period
                Thread.sleep(100);
            }
        }
    }


    private static void readFile(Path filePath) {
        // Read the file content here
        System.out.println("Readin new file...>>>>>>");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()));
            String line;
            while ((line = reader.readLine()) != null) {
                // Process each line in the file
                System.out.println(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
