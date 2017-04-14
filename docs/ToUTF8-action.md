# To UTF-8 Action


Description
-----------
The To UTF-8 Action is used to convert files created in other character sets
into UTF-8 format so that they can be processed using standard Hadoop Text Input Formats.
Due to [MAPREDUCE-232](https://issues.apache.org/jira/browse/MAPREDUCE-232), files created
in other charsets must be converted to UTF-8 before being processed. This plugin supports
any character set listed under ``java.nio`` in 
the [Java Supported Encodings Documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html).

Use Case
--------
This action is used whenever you have any text file that is using a character set other than UTF-8.

Properties
----------
**Source Path:** The full path of the file or directory that is to be converted. In the case of a directory, if
fileRegex is set, then only files in the source directory matching the regex expression will be moved.
Otherwise, all files in the directory will be moved. For example: `hdfs://hostname/tmp`. (Macro-Enabled)

**Destination Path:** The full path where the file or files are to be saved.
If a directory is specified the files will be created in that directory. If the Source Path is
a directory, it is assumed that Destination Path is also a directory. Defaults to the Source Path 
if not specified. The new files will have ``.utf8`` appended to the end. Files with the same name will 
be overwritten. (Macro-Enabled)

**File Regular Expression:** Regular expression to filter the files in the source directory that will be moved. (Macro-Enabled)

**Character Set:** The name of the character set used to create the file. The complete list of supported 
character sets can be found in the [Java Supported Encodings Documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html). (Macro-Enabled)

**Continue Processing If There Are Errors?:** Indicates if the pipeline should continue if processing the files fails. Defaults to false.