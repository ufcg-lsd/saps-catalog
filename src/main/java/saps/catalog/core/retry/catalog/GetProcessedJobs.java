/* (C)2020 */
package saps.catalog.core.retry.catalog;

import java.util.Date;
import java.util.List;
import saps.catalog.core.Catalog;
import saps.common.core.model.SapsUserJob;
import saps.common.core.model.enums.JobState;

public class GetProcessedJobs implements CatalogRetry<List<SapsUserJob>> {

  private Catalog imageStore;
  private String region;
  private Date initDate;
  private Date endDate;

  public GetProcessedTasks(
      Catalog imageStore,
      String lower_left_latitude,
      String lower_left_longitude,
      String upper_right_latitude,
      String upper_right_longitude,
      Date initDate,
      Date endDate
      ) {
    this.imageStore = imageStore;
    this.lower_left_latitude = lower_left_latitude;
    this.lower_left_longitude = lower_left_longitude;
    this.upper_right_latitude = upper_right_latitude;
    this.upper_right_longitude = upper_right_longitude;
    this.initDate = initDate;
    this.endDate = endDate;
  }

  @Override
  public List<SapsImage> run() {
    return imageStore.filterJobs(
        ImageTaskState.ARCHIVED,
        lower_left_latitude,
        lower_left_longitude,
        upper_right_latitude,
        upper_right_longitude,
        initDate,
        endDate);
  }
}
