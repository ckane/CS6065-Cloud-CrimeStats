import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.io.IOException;

public class CrimeRecordInputFormat extends FileInputFormat<Text,CrimeRow> {
  public RecordReader<Text,CrimeRow> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new CrimeRecordReader();
  };

  public class CrimeRecordReader extends RecordReader<Text, CrimeRow> {
    private LineRecordReader lrr = null;
    private Text key = null;
    private CrimeRow val = null;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws
        IOException, InterruptedException {
      lrr = new LineRecordReader();
      lrr.initialize(split, context);
    };

    @Override
    public void close() throws IOException {
      if(lrr != null) {
        lrr.close();
        lrr = null;
      };
      key = null;
      val = null;
    };

    @Override
    public Text getCurrentKey() {
      return key;
    };

    @Override
    public CrimeRow getCurrentValue() {
      return val;
    };

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return lrr.getProgress();
    };

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if(!lrr.nextKeyValue()) {
        /* If there's no more to read, then return false. */
        key = null;
        val = null;
        return false;
      };

      Text line = lrr.getCurrentValue();
      int i = 0;
      boolean quoted_field = false;
      long cur_long = 0;
      int cur_int = 0;
      CrimeRow.CrimeFields fid = CrimeRow.CrimeFields.REPORT;
      String cur_string = "";
      val = new CrimeRow();

      while(i < line.getLength()) {
        if(!quoted_field && line.charAt(i) == ',') {
          fid = val.updateField(fid, cur_long, cur_int, cur_string);
          cur_long = 0;
          cur_int = 0;
          cur_string = "";
        } else if(line.charAt(i) == '"') {
          quoted_field = !quoted_field;
        } else {
          /* Depending upon which field we're on, handle input
             differently. */
          switch(fid) {
            case REPORT:
              cur_long *= 10;
              cur_long += line.charAt(i) - 0x30;
              break;
            case BLOCK_BEGIN:
            case BLOCK_END:
            case DISTRICT:
            case BEAT:
            case AREA:
              cur_int *= 10;
              cur_int += line.charAt(i) - 0x30;
              break;
            default:
              cur_string += Character.toString((char)line.charAt(i));
          };
        };
        i++;
      };

      /* Set the new key as the OFFENSE field. */
      key = new Text(val.offense);

      return true;
    };
  };
};
