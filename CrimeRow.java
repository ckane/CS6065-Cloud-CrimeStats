import java.time.ZonedDateTime;

public class CrimeRow { 
  long rpt_no;
  int bbegin, bend, district, beat, area;
  String offense, orc, street, city, state, neighborhood, dtype,
         officer, badge;
  //ZonedDateTime occurred, reported;
  String occurred, reported;

  public enum CrimeFields {
    REPORT, OFFENSE, ORC, OCCURRED, REPORTED, BLOCK_BEGIN, BLOCK_END,
    STREET, CITY, STATE, DISTRICT, BEAT, AREA, NEIGHBORHOOD, TYPE,
    OFFICER, BADGE
  };

  public CrimeRow() {
    rpt_no = 0L;
    bbegin = bend = district = beat = area = 0;
  };

  CrimeFields updateField(CrimeFields fid, long cur_long, int cur_int, String cur_string) {
    switch(fid) {
      case REPORT:
        rpt_no = cur_long;
        return CrimeFields.OFFENSE;
      case OFFENSE:
        offense = cur_string;
        return CrimeFields.ORC;
      case ORC:
        orc = cur_string;
        return CrimeFields.OCCURRED;
      case OCCURRED:
        occurred = cur_string;
        return CrimeFields.REPORTED;
      case REPORTED:
        reported = cur_string;
        return CrimeFields.BLOCK_BEGIN;
      case BLOCK_BEGIN:
        bbegin = cur_int;
        return CrimeFields.BLOCK_END;
      case BLOCK_END:
        bend = cur_int;
        return CrimeFields.STREET;
      case STREET:
        street = cur_string;
        return CrimeFields.CITY;
      case CITY:
        city = cur_string;
        return CrimeFields.STATE;
      case STATE:
        state = cur_string;
        return CrimeFields.DISTRICT;
      case DISTRICT:
        district = cur_int;
        return CrimeFields.BEAT;
      case BEAT:
        beat = cur_int;
        return CrimeFields.AREA;
      case AREA:
        area = cur_int;
        return CrimeFields.NEIGHBORHOOD;
      case NEIGHBORHOOD:
        neighborhood = cur_string;
        return CrimeFields.TYPE;
      case TYPE:
        dtype = cur_string;
        return CrimeFields.OFFICER;
      case OFFICER:
        officer = cur_string;
        return CrimeFields.BADGE;
      case BADGE:
        badge = cur_string;
    };
    return CrimeFields.REPORT;
  };
};
