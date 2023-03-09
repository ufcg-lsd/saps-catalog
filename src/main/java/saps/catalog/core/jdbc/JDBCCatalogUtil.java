/* (C)2020 */
package saps.catalog.core.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import saps.catalog.core.jdbc.exceptions.JDBCCatalogException;
import saps.common.core.model.SapsImage;
import saps.common.core.model.SapsLandsatImage;
import saps.common.core.model.SapsUser;
import saps.common.core.model.SapsUserJob;
import saps.common.core.model.enums.ImageTaskState;
import saps.common.core.model.enums.JobState;

public class JDBCCatalogUtil {

  public static SapsUser extractSapsUser(ResultSet rs) throws JDBCCatalogException {
    SapsUser sebalUser = null;
    try {
      sebalUser = new SapsUser(
          rs.getString(JDBCCatalogConstants.Tables.User.EMAIL),
          rs.getString(JDBCCatalogConstants.Tables.User.NAME),
          rs.getString(JDBCCatalogConstants.Tables.User.PASSWORD),
          rs.getBoolean(JDBCCatalogConstants.Tables.User.ENABLE),
          rs.getBoolean(JDBCCatalogConstants.Tables.User.NOTIFY),
          rs.getBoolean(JDBCCatalogConstants.Tables.User.ADMIN_ROLE));
    } catch (SQLException e) {
      throw new JDBCCatalogException("Error while extract user", e);
    }

    return sebalUser;
  }

  public static List<SapsImage> extractSapsTasks(ResultSet rs) throws JDBCCatalogException {
    List<SapsImage> imageTasks = new ArrayList<>();
    while (true) {
      try {
        if (!rs.next())
          break;
        imageTasks.add(
            new SapsImage(
                rs.getString(JDBCCatalogConstants.Tables.Task.ID),
                rs.getString(JDBCCatalogConstants.Tables.Task.Image.DATASET),
                rs.getString(JDBCCatalogConstants.Tables.Task.Image.REGION),
                rs.getDate(JDBCCatalogConstants.Tables.Task.Image.DATE),
                ImageTaskState.getStateFromStr(
                    rs.getString(JDBCCatalogConstants.Tables.Task.STATE)),
                rs.getString(JDBCCatalogConstants.Tables.Task.ARREBOL_JOB_ID),
                rs.getString(JDBCCatalogConstants.Tables.Task.FEDERATION_MEMBER),
                rs.getInt(JDBCCatalogConstants.Tables.Task.PRIORITY),
                rs.getString(JDBCCatalogConstants.Tables.User.EMAIL),
                rs.getString(JDBCCatalogConstants.Tables.Task.Algorithms.Inputdownloading.TAG),
                rs.getString(JDBCCatalogConstants.Tables.Task.Algorithms.Inputdownloading.DIGEST),
                rs.getString(JDBCCatalogConstants.Tables.Task.Algorithms.Preprocessing.TAG),
                rs.getString(JDBCCatalogConstants.Tables.Task.Algorithms.Preprocessing.DIGEST),
                rs.getString(JDBCCatalogConstants.Tables.Task.Algorithms.Processing.TAG),
                rs.getString(JDBCCatalogConstants.Tables.Task.Algorithms.Processing.DIGEST),
                rs.getTimestamp(JDBCCatalogConstants.Tables.Task.CREATION_TIME),
                rs.getTimestamp(JDBCCatalogConstants.Tables.Task.UPDATED_TIME),
                rs.getString(JDBCCatalogConstants.Tables.Task.STATUS),
                rs.getString(JDBCCatalogConstants.Tables.Task.ERROR_MSG)));
      } catch (SQLException e) {
        throw new JDBCCatalogException("Error while extract task", e);
      }
    }
    return imageTasks;
  }

  public static List<SapsUserJob> extractSapsUserJob(ResultSet rs, boolean withoutTasks) throws JDBCCatalogException {
    List<SapsUserJob> userJobs = new ArrayList<SapsUserJob>();
    while (true) {
      try {
        if (!rs.next())
          break;

        List<String> tasksIds = new ArrayList<>();
        if (!withoutTasks)
          tasksIds = Arrays.asList(
              rs.getString(JDBCCatalogConstants.Tables.Job.TASKS_IDS).replace("{", "").replace("}", "").split(","));

        userJobs.add(
            new SapsUserJob(
                rs.getString(JDBCCatalogConstants.Tables.Job.ID),
                rs.getString(JDBCCatalogConstants.Tables.Job.LOWER_LEFT_LATITUDE),
                rs.getString(JDBCCatalogConstants.Tables.Job.LOWER_LEFT_LONGITUDE),
                rs.getString(JDBCCatalogConstants.Tables.Job.UPPER_RIGHT_LATITUDE),
                rs.getString(JDBCCatalogConstants.Tables.Job.UPPER_RIGHT_LONGITUDE),
                JobState.getStateFromStr(rs.getString(JDBCCatalogConstants.Tables.Job.STATE)),
                rs.getString(JDBCCatalogConstants.Tables.Job.USER_EMAIL),
                rs.getString(JDBCCatalogConstants.Tables.Job.JOB_LABEL),
                rs.getDate(JDBCCatalogConstants.Tables.Job.START_DATE),
                rs.getDate(JDBCCatalogConstants.Tables.Job.END_DATE),
                rs.getInt(JDBCCatalogConstants.Tables.Job.PRIORITY),
                tasksIds,
                rs.getTimestamp(JDBCCatalogConstants.Tables.Job.CREATION_TIME)));
      } catch (SQLException e) {
        throw new JDBCCatalogException("Error while extract task", e);
      }
    }
    return userJobs;
  }

  public static SapsLandsatImage extractSapsImages(ResultSet rs) throws JDBCCatalogException {
    SapsLandsatImage sapsLandImage = null;
    while (true) {
      try {
        if (!rs.next())
          break;
        sapsLandImage = new SapsLandsatImage(
            rs.getString(JDBCCatalogConstants.Tables.LandsatImages.LANDSAT_KEY));
      } catch (SQLException e) {
        throw new JDBCCatalogException("Error while extract landsat images", e);
      }
    }
    return sapsLandImage;
  }
}
