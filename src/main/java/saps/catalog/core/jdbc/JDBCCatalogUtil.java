/* (C)2020 */
package saps.catalog.core.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import saps.catalog.core.jdbc.exceptions.JDBCCatalogException;
import saps.common.core.model.SapsImage;
import saps.common.core.model.SapsLandsatImage;
import saps.common.core.model.SapsUser;
import saps.common.core.model.enums.ImageTaskState;

public class JDBCCatalogUtil {

  public static SapsUser extractSapsUser(ResultSet rs) throws JDBCCatalogException {
    SapsUser sebalUser = null;
    try {
      sebalUser =
          new SapsUser(
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
        if (!rs.next()) break;
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

  public static List<SapsLandsatImage> extractSapsImages(ResultSet rs) throws JDBCCatalogException {
    List<SapsLandsatImage> validImages = new ArrayList<>();
    while(true) {
      try {
        if (!rs.next()) break;
        validImages.add(
          new SapsLandsatImage(
          rs.getString(JDBCCatalogConstants.Tables.LandsatImages.PRODUCT_ID),
          rs.getDate(JDBCCatalogConstants.Tables.LandsatImages.DATE),
          rs.getString(JDBCCatalogConstants.Tables.LandsatImages.DATASET)));
      } catch (SQLException e) {
        throw new JDBCCatalogException("Error while extract landsat images", e);
      }
    } return validImages;
  }
}
