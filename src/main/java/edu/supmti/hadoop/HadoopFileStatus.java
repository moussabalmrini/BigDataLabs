package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {
        if(args.length < 2) {
            System.out.println("Usage: HadoopFileStatus <full_file_path> <new_name>");
            System.exit(1);
        }

        String filePathStr = args[0]; // المسار الكامل للملف
        String newName = args[1];     // الاسم الجديد للملف

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(filePathStr);

        if(!fs.exists(filePath) || !fs.isFile(filePath)){
            System.out.println("File does not exist or is not a file: " + filePathStr);
            System.exit(1);
        }

        FileStatus status = fs.getFileStatus(filePath);

        System.out.println("File Name: " + filePath.getName());
        System.out.println("File Size: " + status.getLen() + " bytes");
        System.out.println("File Owner: " + status.getOwner());
        System.out.println("File Permission: " + status.getPermission());
        System.out.println("File Replication: " + status.getReplication());
        System.out.println("File Block Size: " + status.getBlockSize());

        BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
        for(BlockLocation blockLocation : blockLocations) {
            System.out.println("Block offset: " + blockLocation.getOffset());
            System.out.println("Block length: " + blockLocation.getLength());
            System.out.print("Block hosts: ");
            for(String host : blockLocation.getHosts()) {
                System.out.print(host + " ");
            }
            System.out.println();
        }

        // Rename file (in the same directory)
        Path parentDir = filePath.getParent();
        fs.rename(filePath, new Path(parentDir, newName));
        fs.close();
    }
}
