package org.apache.shell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

public class TestFSShellCluster {
  public static void main(String[] args){
    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "hdfs://localhost:9000");
    configuration.set("dfs.replication", "1");
    FsShell fsShell = new FsShell(configuration);
    String[] shellArgs = {"-ls", "/"};
    fsShell.run(shellArgs);
    try {
      FileSystem fileSystem = FileSystem.get(configuration);
      RemoteIterator<LocatedFileStatus> fileListIterator = fileSystem.listFiles( new Path("/"), false);
      while(fileListIterator.hasNext()){
        System.out.println(fileListIterator.next().getPath());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}

