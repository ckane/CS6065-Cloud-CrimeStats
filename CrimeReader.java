import java.io.BufferedReader;
import java.io.Reader;
import java.io.IOException;

public class CrimeReader extends BufferedReader {
  public CrimeReader(Reader in) throws IOException {
    super(in);
    /* Consume the first line of the file, so that we begin with the
       data lines. */
    while(read() != 0x0a) {};
  };

  CrimeRow getNext() throws IOException {
    CrimeRow ncrime = new CrimeRow();
    int c;
    long cur_long = 0;
    int cur_int = 0;
    String cur_string = "";
    CrimeRow.CrimeFields fid = CrimeRow.CrimeFields.REPORT;
    boolean quoted_field = false;
    
    while((c = read()) != 0x0a) {
      if(!quoted_field && c == ',') {
        fid = ncrime.updateField(fid, cur_long, cur_int, cur_string);
        cur_long = 0;
        cur_int = 0;
        cur_string = "";
      } else if(c == '"') {
        quoted_field = !quoted_field;
      } else if(c == -1) {
        break;
      } else {
        /* Depending upon which field we're on, handle input
           differently. */
        switch(fid) {
          case REPORT:
            cur_long *= 10;
            cur_long += c - 0x30;
            break;
          case BLOCK_BEGIN:
          case BLOCK_END:
          case DISTRICT:
          case BEAT:
          case AREA:
            cur_int *= 10;
            cur_int += c - 0x30;
            break;
          default:
            cur_string += Character.toString((char)c);
        };
      };
    };

    /* Update the final field. */
    ncrime.updateField(fid, cur_long, cur_int, cur_string);

    return ncrime;
  };
};
