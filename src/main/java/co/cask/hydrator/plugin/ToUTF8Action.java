/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.regex.Pattern;
import javax.annotation.Nullable;


/**
 * ToUTF8 Action Plugin - Converts the specified file to UTF-8.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(ToUTF8Action.PLUGIN_NAME)
@Description("Converts the specified files from a specified charset to UTF-8.")
public class ToUTF8Action extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(ToUTF8Action.class);
  private static final int BUFFER_SIZE = 4096;

  public static final String PLUGIN_NAME = "ToUTF8";

  private final ToUTF8Config config;

  public ToUTF8Action(ToUTF8Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate();
    Path source = new Path(config.sourceFilePath);
    Path dest = new Path(config.destFilePath);

    FileSystem fileSystem = source.getFileSystem(new Configuration());
    fileSystem.mkdirs(dest.getParent());

    // Convert a single file
    if (fileSystem.getFileStatus(source).isFile()) {
      convertSingleFile(source, dest, fileSystem);
    } else {
      // Convert all the files in a directory
      PathFilter filter = new PathFilter() {
        private final Pattern pattern = Pattern.compile(config.fileRegex);

        @Override
        public boolean accept(Path path) {
          return pattern.matcher(path.getName()).matches();
        }
      };
      FileStatus[] listFiles = fileSystem.listStatus(source, filter);

      if (listFiles.length == 0) {
        LOG.warn("Not converting any files from source {} matching regular expression",
                 source.toString(), config.fileRegex);
      }

      if (fileSystem.isFile(dest)) {
        throw new IllegalArgumentException(
          String.format("Destination %s needs to be a directory since the source is a " +
                          "directory", config.destFilePath));
      }
      // create destination directory if necessary
      fileSystem.mkdirs(dest);

      for (FileStatus file : listFiles) {
        source = file.getPath();
        convertSingleFile(source, new Path(dest.toString() + source.getName() + ".utf8"), fileSystem);
      }
    }
  }

  private void convertSingleFile(Path source, Path dest, FileSystem fileSystem) throws IOException {
    Path actualDestPath = (fileSystem.isDirectory(dest))
      ? new Path(dest.toString() + source.getName() + ".utf8")
      : dest;
    try (InputStream in = fileSystem.open(source);
         BufferedOutputStream out = new BufferedOutputStream(fileSystem.create(actualDestPath), BUFFER_SIZE)) {
      byte[] tempBytes = new byte[BUFFER_SIZE];
      int available = -1;
      while ((available = in.read(tempBytes)) > 0) {
        String tempString = Bytes.toString(ByteBuffer.wrap(tempBytes), Charset.forName(config.charset));
        out.write(Bytes.toBytes(tempString), 0, available);
      }
      out.flush();
      out.close();
    } catch (IOException e) {
      if (!config.continueOnError) {
        throw new IOException(String.format("Failed to convert file %s to %s", source.toString(), dest.toString()));
      }
      LOG.warn(String.format("Exception convert file %s to %s", source.toString(), dest.toString()), e);
    }
  }

  /**
   * Config class that contains all properties required for running the unload command.
   */
  public static class ToUTF8Config extends PluginConfig {
    @Macro
    @Description("The source location where the file or files live.")
    private String sourceFilePath;

    @Macro
    @Nullable
    @Description("The destination location where the converted files should be. Defaults to the sourceFilePath.")
    private String destFilePath;

    @Macro
    @Description("The source charset.")
    private String charset;

    @Macro
    @Nullable
    @Description("A regular expression for filtering files such as .*\\.txt")
    private String fileRegex;

    @Macro
    @Nullable
    @Description("Set to true if this plugin should ignore errors.")
    private Boolean continueOnError;


    public ToUTF8Config(String sourceFilePath, @Nullable String destFilePath, @Nullable String fileRegex,
                        String charset, @Nullable Boolean continueOnError) {
      this.sourceFilePath = sourceFilePath;
      this.charset = charset;
      this.destFilePath = (Strings.isNullOrEmpty(destFilePath)) ? sourceFilePath : destFilePath;
      this.continueOnError = (continueOnError == null) ? false : continueOnError;
      this.fileRegex = (Strings.isNullOrEmpty(fileRegex)) ? ".*" : fileRegex;
    }

    /**
     * Validates the config parameters required for unloading the data.
     */
    private void validate() {

    }
  }
}
