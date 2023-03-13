/* (C)2020 */
package saps.catalog.core.jdbc;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;
import saps.catalog.core.Catalog;
import saps.catalog.core.exceptions.CatalogException;
import saps.catalog.core.exceptions.TaskNotFoundException;
import saps.catalog.core.exceptions.UserNotFoundException;
import saps.catalog.core.jdbc.exceptions.JDBCCatalogException;
import saps.common.core.model.SapsImage;
import saps.common.core.model.SapsLandsatImage;
import saps.common.core.model.SapsUser;
import saps.common.core.model.SapsUserJob;
import saps.common.core.model.enums.ImageTaskState;
import saps.common.core.model.enums.JobState;
import saps.common.utils.SapsPropertiesUtil;

public class JDBCCatalog implements Catalog {

  private static final Logger LOGGER = Logger.getLogger(JDBCCatalog.class);

  private final BasicDataSource connectionPool;

  public JDBCCatalog(Properties properties) throws CatalogException {
    if (!checkProperties(properties))
      throw new CatalogException(
          "Error on validate the file. Missing properties for start JDBC Catalog.");

    String dbIP = properties.getProperty(JDBCCatalogConstants.Database.IP);
    String dbPort = properties.getProperty(JDBCCatalogConstants.Database.PORT);
    String dbURLPrefix = properties.getProperty(JDBCCatalogConstants.Database.URL_PREFIX);
    String dbUserName = properties.getProperty(JDBCCatalogConstants.Database.USERNAME);
    String dbUserPass = properties.getProperty(JDBCCatalogConstants.Database.PASSWORD);
    String dbDrive = properties.getProperty(JDBCCatalogConstants.Database.DRIVER);
    String dbName = properties.getProperty(JDBCCatalogConstants.Database.NAME);

    LOGGER.info("Creating connection pool for Catalog " + dbIP + ":" + dbPort);
    this.connectionPool = createConnectionPool(dbURLPrefix, dbIP, dbPort, dbUserName, dbUserPass, dbDrive, dbName);

    LOGGER.info("Creating (if not exists) tables for SAPS schema");
    createTable();

    LOGGER.info("JDBC Catalog class created");
  }

  private boolean checkProperties(Properties properties) {
    String[] propertiesSet = {
        JDBCCatalogConstants.Database.IP,
        JDBCCatalogConstants.Database.USERNAME,
        JDBCCatalogConstants.Database.PASSWORD,
        JDBCCatalogConstants.Database.DRIVER,
        JDBCCatalogConstants.Database.NAME
    };

    return SapsPropertiesUtil.checkProperties(properties, propertiesSet);
  }

  private void createTable() throws CatalogException {

    Connection connection = null;
    Statement statement = null;

    try {
      connection = getConnection();
      statement = connection.createStatement();

      statement.execute(JDBCCatalogConstants.CreateTable.USERS);
      statement.execute(JDBCCatalogConstants.CreateTable.TASKS);
      statement.execute(JDBCCatalogConstants.CreateTable.TIMESTAMPS);
      statement.execute(JDBCCatalogConstants.CreateTable.NOTIFY);
      statement.execute(JDBCCatalogConstants.CreateTable.DEPLOY_CONFIG);
      statement.execute(JDBCCatalogConstants.CreateTable.PROVENANCE_DATA);
      statement.execute(JDBCCatalogConstants.CreateTable.LANDSAT_IMAGES);
      statement.execute(JDBCCatalogConstants.CreateTable.JOBS);

      statement.close();
    } catch (SQLException e) {
      LOGGER.error("Error while initializing DataStore", e);
      throw new CatalogException("Error while initializing DataStore");
    } finally {
      close(statement, connection);
    }
  }

  private BasicDataSource createConnectionPool(
      String dbURLPrefix,
      String dbIP,
      String dbPort,
      String dbUserName,
      String dbUserPass,
      String dbDriver,
      String dbName) {
    String url = dbURLPrefix + dbIP + ":" + dbPort + "/" + dbName;

    LOGGER.debug("Catalog URL: " + url);

    BasicDataSource pool = new BasicDataSource();
    pool.setUsername(dbUserName);
    pool.setPassword(dbUserPass);
    pool.setDriverClassName(dbDriver);
    pool.setUrl(url);
    pool.setInitialSize(1);

    return pool;
  }

  public Connection getConnection() throws CatalogException {
    try {
      return connectionPool.getConnection();
    } catch (SQLException e) {
      LOGGER.error("Error while getting a new connection from the connection pool", e);
      throw new CatalogException("Error while getting a new connection from the connection pool");
    }
  }

  protected void close(Statement statement, Connection conn) {
    close(statement);

    if (conn != null) {
      try {
        if (!conn.isClosed()) {
          conn.close();
        }
      } catch (SQLException e) {
        LOGGER.error("Couldn't close connection", e);
      }
    }
  }

  private void close(Statement statement) {
    if (statement != null) {
      try {
        if (!statement.isClosed()) {
          statement.close();
        }
      } catch (SQLException e) {
        LOGGER.error("Couldn't close statement", e);
      }
    }
  }

  private java.sql.Date javaDateToSqlDate(Date date) {
    return new java.sql.Date(date.getTime());
  }

  // == Users
  @Override
  public void addUser(
      String userEmail,
      String userName,
      String userPass,
      boolean isEnable,
      boolean userNotify,
      boolean adminRole)
      throws CatalogException {

    LOGGER.info("Adding user " + userName + " into DB");
    if (userName == null || userName.isEmpty() || userPass == null || userPass.isEmpty()) {
      throw new IllegalArgumentException("Unable to create user with empty name or password.");
    }

    PreparedStatement insertStatement = null;
    Connection connection = null;

    try {
      connection = getConnection();

      insertStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Insert.USER);
      insertStatement.setString(1, userEmail);
      insertStatement.setString(2, userName);
      insertStatement.setString(3, userPass);
      insertStatement.setBoolean(4, isEnable);
      insertStatement.setBoolean(5, userNotify);
      insertStatement.setBoolean(6, adminRole);
      insertStatement.setQueryTimeout(300);

      insertStatement.execute();
    } catch (SQLException e) {
      throw new CatalogException("Error while try add a new user");
    } finally {
      close(insertStatement, connection);
    }
  }

  @Override
  public SapsUser getUserByEmail(String userEmail) throws CatalogException, UserNotFoundException {

    if (userEmail == null || userEmail.isEmpty()) {
      LOGGER.error("Invalid userEmail " + userEmail);
      return null;
    }
    PreparedStatement selectStatement = null;
    Connection connection = null;

    try {
      connection = getConnection();

      selectStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Select.USER);
      selectStatement.setString(1, userEmail);
      selectStatement.setQueryTimeout(300);

      selectStatement.execute();

      ResultSet rs = selectStatement.getResultSet();
      if (rs.next()) {
        SapsUser sebalUser = JDBCCatalogUtil.extractSapsUser(rs);
        return sebalUser;
      }
      rs.close();
      throw new UserNotFoundException("There is no user with email");
    } catch (SQLException e) {
      throw new CatalogException("Error while getting user by email");
    } catch (JDBCCatalogException e) {
      throw new CatalogException("Error while extract user");
    } finally {
      close(selectStatement, connection);
    }
  }

  // == Tasks
  @Override
  public SapsImage addTask(
      String taskId,
      String dataset,
      String region,
      Date date,
      int priority,
      String user,
      String inputdownloadingPhaseTag,
      String digestInputdownloading,
      String preprocessingPhaseTag,
      String digestPreprocessing,
      String processingPhaseTag,
      String digestProcessing)
      throws CatalogException {
    Timestamp now = new Timestamp(System.currentTimeMillis());
    SapsImage task = new SapsImage(
        taskId,
        dataset,
        region,
        date,
        ImageTaskState.CREATED,
        SapsImage.NONE_ARREBOL_JOB_ID,
        SapsImage.NONE_FEDERATION_MEMBER,
        priority,
        user,
        inputdownloadingPhaseTag,
        digestInputdownloading,
        preprocessingPhaseTag,
        digestPreprocessing,
        processingPhaseTag,
        digestProcessing,
        now,
        now,
        SapsImage.AVAILABLE,
        SapsImage.NON_EXISTENT_DATA);

    if (task.getTaskId() == null || task.getTaskId().isEmpty()) {
      LOGGER.error("Task with empty id.");
      throw new IllegalArgumentException("Task with empty id.");
    }
    if (task.getDataset() == null || task.getDataset().isEmpty()) {
      LOGGER.error("Task with empty dataset.");
      throw new IllegalArgumentException("Task with empty dataset.");
    }
    if (task.getImageDate() == null) {
      LOGGER.error("Task must have a date.");
      throw new IllegalArgumentException("Task must have a date.");
    }
    if (task.getUser() == null || task.getUser().isEmpty()) {
      LOGGER.error("Task must have a user.");
      throw new IllegalArgumentException("Task must have a user.");
    }

    LOGGER.info("Adding image task " + task.getTaskId() + " with priority " + task.getPriority());
    LOGGER.info(task.toString());

    PreparedStatement insertStatement = null;
    Connection connection = null;

    try {
      connection = getConnection();

      insertStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Insert.TASK);
      insertStatement.setString(1, task.getTaskId());
      insertStatement.setString(2, task.getDataset());
      insertStatement.setString(3, task.getRegion());
      insertStatement.setDate(4, javaDateToSqlDate(task.getImageDate()));
      insertStatement.setString(5, task.getState().getValue());
      insertStatement.setString(6, task.getArrebolJobId());
      insertStatement.setString(7, task.getFederationMember());
      insertStatement.setInt(8, task.getPriority());
      insertStatement.setString(9, task.getUser());
      insertStatement.setString(10, task.getInputdownloadingTag());
      insertStatement.setString(11, task.getDigestInputdownloading());
      insertStatement.setString(12, task.getPreprocessingTag());
      insertStatement.setString(13, task.getDigestPreprocessing());
      insertStatement.setString(14, task.getProcessingTag());
      insertStatement.setString(15, task.getDigestProcessing());
      insertStatement.setTimestamp(16, task.getCreationTime());
      insertStatement.setTimestamp(17, task.getUpdateTime());
      insertStatement.setString(18, task.getStatus());
      insertStatement.setString(19, task.getError());
      insertStatement.setQueryTimeout(300);

      insertStatement.execute();
    } catch (SQLException e) {
      throw new CatalogException("Error while insert a new task");
    } finally {
      close(insertStatement, connection);
    }

    return task;
  }

  @Override
  public void updateImageTask(SapsImage imagetask) throws CatalogException {
    if (imagetask == null) {
      LOGGER.error("Trying to update null image task.");
      throw new IllegalArgumentException("Trying to update null image task.");
    }

    PreparedStatement updateStatement = null;
    Connection connection = null;

    try {
      connection = getConnection();

      updateStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Update.TASK);
      updateStatement.setString(1, imagetask.getState().getValue());
      updateStatement.setString(2, imagetask.getStatus());
      updateStatement.setString(3, imagetask.getError());
      updateStatement.setString(4, imagetask.getArrebolJobId());
      updateStatement.setString(5, imagetask.getTaskId());
      updateStatement.setQueryTimeout(300);

      updateStatement.execute();
    } catch (SQLException e) {
      throw new CatalogException("Error while try updates task");
    } finally {
      close(updateStatement, connection);
    }
  }

  @Override
  public List<SapsImage> getAllTasks() throws CatalogException {
    Statement statement = null;
    Connection conn = null;
    try {
      conn = getConnection();
      statement = conn.createStatement();
      statement.setQueryTimeout(300);

      statement.execute(JDBCCatalogConstants.Queries.Select.TASKS);
      ResultSet rs = statement.getResultSet();
      return JDBCCatalogUtil.extractSapsTasks(rs);
    } catch (SQLException e) {
      throw new CatalogException("Error while select all tasks");
    } catch (JDBCCatalogException e) {
      throw new CatalogException("Error while extract all tasks");
    } finally {
      close(statement, conn);
    }
  }

  private String buildTaskByStateQuery(int states) {
    StringBuilder query = new StringBuilder(JDBCCatalogConstants.Queries.Select.TASKS + " WHERE state in (");
    for (int i = 0; i < states; i++) {
      if (i == states - 1) {
        query.append("?) ");
      } else {
        query.append("?,");
      }
    }
    query.append("ORDER BY priority asc");
    return query.toString();
  }

  @Override
  public List<SapsImage> getTasksByState(ImageTaskState... tasksStates) throws CatalogException {
    if (tasksStates == null) {
      LOGGER.error("A state must be given");
      throw new IllegalArgumentException("Can't recover tasks. State was null.");
    }

    PreparedStatement selectStatement = null;
    Connection connection = null;
    try {
      connection = getConnection();

      String query = buildTaskByStateQuery(tasksStates.length);
      selectStatement = connection.prepareStatement(query);
      selectStatement.setQueryTimeout(300);

      for (int i = 0; i < tasksStates.length; i++) {
        selectStatement.setString(i + 1, tasksStates[i].getValue());
      }

      selectStatement.execute();

      ResultSet rs = selectStatement.getResultSet();
      List<SapsImage> imageDatas = JDBCCatalogUtil.extractSapsTasks(rs);
      rs.close();
      return imageDatas;
    } catch (SQLException e) {
      throw new CatalogException("Error while getting task by state");
    } catch (JDBCCatalogException e) {
      throw new CatalogException("Error while extract all tasks");
    } finally {
      close(selectStatement, connection);
    }
  }

  @Override
  public SapsImage getTaskById(String taskId) throws CatalogException, TaskNotFoundException {
    if (taskId == null) {
      LOGGER.error("Null image task");
      throw new IllegalArgumentException("Invalid image task null");
    }

    if (taskId.isEmpty()) {
      LOGGER.error("Invalid image task " + taskId);
      throw new IllegalArgumentException("Invalid image task " + taskId);
    }

    PreparedStatement selectStatement = null;
    Connection connection = null;

    try {
      connection = getConnection();

      selectStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Select.TASK);
      selectStatement.setString(1, taskId);
      selectStatement.setQueryTimeout(300);

      selectStatement.execute();

      ResultSet rs = selectStatement.getResultSet();
      List<SapsImage> imageDatas = JDBCCatalogUtil.extractSapsTasks(rs);

      if (imageDatas.size() == 0)
        throw new TaskNotFoundException("There is no task with id");

      rs.close();
      return imageDatas.get(0);
    } catch (SQLException e) {
      throw new CatalogException("Error while getting task by id");
    } catch (JDBCCatalogException e) {
      throw new CatalogException("Error while extract all tasks");
    } finally {
      close(selectStatement, connection);
    }
  }

  @Override
  public List<SapsImage> filterTasks(
      ImageTaskState state,
      String region,
      Date initDate,
      Date endDate,
      String inputGathering,
      String preprocessingTag,
      String processingTag)
      throws CatalogException {
    PreparedStatement queryStatement = null;
    Connection connection = null;

    try {
      connection = getConnection();

      queryStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Select.FILTER_TASKS);
      queryStatement.setString(1, state.getValue());
      queryStatement.setString(2, region);
      queryStatement.setDate(3, javaDateToSqlDate(initDate));
      queryStatement.setDate(4, javaDateToSqlDate(endDate));
      queryStatement.setString(5, preprocessingTag);
      queryStatement.setString(6, inputGathering);
      queryStatement.setString(7, processingTag);
      queryStatement.setQueryTimeout(300);

      ResultSet result = queryStatement.executeQuery();
      return JDBCCatalogUtil.extractSapsTasks(result);
    } catch (SQLException e) {
      throw new CatalogException("Error while getting tasks by filters");
    } catch (JDBCCatalogException e) {
      throw new CatalogException("Error while extract all tasks");
    } finally {
      close(queryStatement, connection);
    }
  }

  // == Landsat
  @Override
  public SapsLandsatImage getLandsatImages(String region, Date date) throws CatalogException {

    if (region == null || region.isEmpty()) {
      LOGGER.error("Invalid region " + region);
      throw new IllegalArgumentException("Region is missing");
    }

    if (date == null) {
      LOGGER.error("Invalid date " + date);
      throw new NullPointerException("Invalid date (null)");
    }

    PreparedStatement statement = null;
    Connection connection = null;

    java.sql.Date sqlDate = javaDateToSqlDate(date);
    String strDate = sqlDate.toString();
    String regionAndDate = region + strDate.replace("-", "");

    long regionAsLong = Long.parseLong(regionAndDate);

    try {
      connection = getConnection();
      statement = connection.prepareStatement(JDBCCatalogConstants.Queries.Select.LANDSAT_IMAGES);
      statement.setLong(1, regionAsLong);
      statement.setQueryTimeout(300);

      statement.execute();

      ResultSet rs = statement.getResultSet();
      SapsLandsatImage result = JDBCCatalogUtil.extractSapsImages(rs);
      rs.close();
      return result;

    } catch (SQLException e) {
      throw new CatalogException("Erro while select landsat images", e);
    } catch (JDBCCatalogException e) {
      throw new CatalogException("Error while getting landsat images", e);
    } finally {
      close(statement, connection);
    }
  }

  // == Timestamp
  @Override
  public void addStateChangeTime(String taskId, ImageTaskState state, Timestamp timestamp)
      throws CatalogException {
    if (taskId == null || taskId.isEmpty() || state == null) {
      LOGGER.error("Task id or state was null.");
      throw new IllegalArgumentException("Task id or state was null.");
    }
    LOGGER.info(
        "Adding task "
            + taskId
            + " state "
            + state.getValue()
            + " with timestamp "
            + timestamp
            + " into Catalogue");

    PreparedStatement insertStatement = null;
    Connection connection = null;

    try {
      connection = getConnection();

      insertStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Insert.TIMESTAMP);
      insertStatement.setString(1, taskId);
      insertStatement.setString(2, state.getValue());
      insertStatement.setQueryTimeout(300);

      insertStatement.execute();
    } catch (SQLException e) {
      throw new CatalogException("Error while add a new state change time");
    } finally {
      close(insertStatement, connection);
    }
  }

  @Override
  public void removeStateChangeTime(String taskId, ImageTaskState state, Timestamp timestamp)
      throws CatalogException {

    if (taskId == null || taskId.isEmpty() || state == null || state.getValue() == null) {
      if (state != null) {
        LOGGER.error("Invalid task " + taskId + " or state with value " + state.getValue());
      }
      LOGGER.error("Invalid task " + taskId + " or state " + state);
      throw new IllegalArgumentException("Invalid task " + taskId);
    }

    LOGGER.info(
        "Removing task " + taskId + " state " + state.getValue() + " with timestamp " + timestamp);
    PreparedStatement removeStatement = null;
    Connection connection = null;

    try {
      connection = getConnection();

      removeStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Delete.TIMESTAMP);
      removeStatement.setString(1, taskId);
      removeStatement.setString(2, state.getValue());
      removeStatement.setTimestamp(3, timestamp);
      removeStatement.setQueryTimeout(300);

      removeStatement.execute();
    } catch (SQLException e) {
      throw new CatalogException("Error while removes state change time");
    } finally {
      close(removeStatement, connection);
    }
  }

  // == Jobs
  @Override
  public SapsUserJob addJob(
      String jobId,
      String lowerLeftLatitude,
      String lowerLeftLongitude,
      String upperRightLatitude,
      String upperRightLongitude,
      String userEmail,
      String jobLabel,
      Date startDate,
      Date endDate,
      int priority,
      List<String> taskIds)
      throws CatalogException {
    Timestamp now = new Timestamp(System.currentTimeMillis());
    SapsUserJob userJob = new SapsUserJob(
        jobId,
        lowerLeftLatitude,
        lowerLeftLongitude,
        upperRightLatitude,
        upperRightLongitude,
        JobState.SUBMITTED,
        userEmail,
        jobLabel,
        startDate,
        endDate,
        priority,
        taskIds,
        now);

    if (jobId == null || jobId.isEmpty()) {
      LOGGER.error("job with empty id");
      throw new IllegalArgumentException("Job with empty id.");
    }

    if (lowerLeftLatitude == null || lowerLeftLatitude.isEmpty()
        || lowerLeftLongitude == null || lowerLeftLongitude.isEmpty()
        || upperRightLatitude == null || upperRightLatitude.isEmpty()
        || upperRightLongitude == null || upperRightLongitude.isEmpty()) {
      LOGGER.error("job with invalid coordinates");
      throw new IllegalArgumentException("Job with invalid coordinates.");
    }

    if (userEmail == null || userEmail.isEmpty()) {
      LOGGER.error("job must have an user");
      throw new IllegalArgumentException("Job must have a valid user.");
    }

    LOGGER.info("Adding job " + userJob.getJobId() + " with priority " + userJob.getPriority());

    PreparedStatement insertStatement = null;
    Connection connection = null;

    try {
      connection = getConnection();
      insertStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Insert.JOB);

      String[] arr = userJob.getTaskIds().toArray(new String[userJob.getTaskIds().size()]);
      Array taskIdsArr = connection.createArrayOf("text", arr);

      insertStatement.setString(1, userJob.getJobId());
      insertStatement.setString(2, userJob.getLowerLeftLatitude());
      insertStatement.setString(3, userJob.getLowerLeftLongitude());
      insertStatement.setString(4, userJob.getUpperRightLatitude());
      insertStatement.setString(5, userJob.getUpperRightLongitude());
      insertStatement.setString(6, userJob.getState().toString());
      insertStatement.setString(7, userJob.getUserEmail());
      insertStatement.setString(8, userJob.getJobLabel());
      insertStatement.setDate(9, javaDateToSqlDate(userJob.getStartDate()));
      insertStatement.setDate(10, javaDateToSqlDate(userJob.getEndDate()));
      insertStatement.setInt(11, userJob.getPriority());
      insertStatement.setArray(12, taskIdsArr);
      insertStatement.setQueryTimeout(300);

      insertStatement.execute();
    } catch (SQLException e) {
      throw new CatalogException("Error while insert a new job");
    } finally {
      close(insertStatement, connection);
    }

    return userJob;
  }

  @Override
  public void updateUserJob(String jobId, JobState state) throws CatalogException {
    if (jobId == null || jobId.isEmpty()) {
      LOGGER.error("job with empty id");
      throw new IllegalArgumentException("Job with empty id.");
    }

    Connection connection = null;
    PreparedStatement updateStatement = null;
    try {
      connection = getConnection();

      updateStatement = connection.prepareStatement(JDBCCatalogConstants.Queries.Update.JOB);
      updateStatement.setString(1, state.value());
      updateStatement.setString(2, jobId);
      updateStatement.setQueryTimeout(300);

      updateStatement.execute();
    } catch (SQLException e) {
      throw new CatalogException("Error while try to update job");
    } finally {
      close(updateStatement, connection);
    }
  }

  private String addUserJobFilter(String search, JobState state, Integer page, Integer size, String sortField, String sortOrder,
      boolean recoverOngoing, boolean recoverCompleted) {
    StringBuilder query = new StringBuilder();

    if (search != null && !search.trim().isEmpty()) {
      query.append(" WHERE job_label LIKE '" + search + "%' ");
    } else if (recoverOngoing) {
      query.append(" WHERE (state <> '" + JobState.FAILED.value() + "' AND state <> '" + JobState.FINISHED.value() + "') ");
    } else if (recoverCompleted) {
      query.append(" WHERE (state = '" + JobState.FAILED.value() + "' OR state = '" + JobState.FINISHED.value() + "') ");
    } else if (state != null) {
      query.append(" WHERE state = '" + state.value() + "' ");
    }

    if (sortField != null && sortOrder != null && !sortField.trim().isEmpty() && !sortOrder.trim().isEmpty())
      query.append(" ORDER BY " + sortField + " " + sortOrder.toUpperCase());
    if (page > 0 && size > 0)
      query.append(" OFFSET " + (page - 1) * size + " ROWS FETCH NEXT " + size + "  ROWS ONLY");

    return query.toString();
  }

  @Override
  public List<SapsUserJob> getUserJobs(
      JobState state,
      String search,
      Integer page,
      Integer size,
      String sortField,
      String sortOrder,
      boolean withoutTasks,
      boolean recoverOngoing,
      boolean recoverCompleted)
      throws CatalogException {

    PreparedStatement statement = null;
    Connection conn = null;

    StringBuilder query = new StringBuilder(JDBCCatalogConstants.Queries.Select.JOBS);
    query.append(addUserJobFilter(search, state, page, size, sortField, sortOrder, recoverOngoing, recoverCompleted));

    try {
      conn = getConnection();
      statement = conn.prepareStatement(query.toString());
      statement.setQueryTimeout(300);
      statement.execute();

      ResultSet rs = statement.getResultSet();
      return JDBCCatalogUtil.extractSapsUserJob(rs, withoutTasks);
    } catch (SQLException e) {
      throw new CatalogException("Error while extract all jobs");
    } catch (JDBCCatalogException e) {
      throw new CatalogException("Error while extract all tasks");
    } finally {
      close(statement, conn);
    }
  }

  @Override
  public Integer getUserJobsCount(JobState state, String search, boolean recoverOngoing, boolean recoverCompleted) throws CatalogException {
    PreparedStatement statement = null;
    Connection connection = null;

    StringBuilder query = new StringBuilder(JDBCCatalogConstants.Queries.Select.JOBS_COUNT);
    query.append(addUserJobFilter(search, state, 0, 0, null, null, recoverOngoing, recoverCompleted));

    try {
      connection = getConnection();
      statement = connection.prepareStatement(query.toString());
      statement.setQueryTimeout(300);
      statement.execute();

      ResultSet rs = statement.getResultSet();
      rs.next();
      return rs.getInt("count");
    } catch (SQLException e) {
      throw new CatalogException("Error while select all tasks");
    } finally {
      close(statement, connection);
    }
  }

  // == Jobs tasks

  private String addJobTasksFilter(String search, ImageTaskState state, Integer page, Integer size,
      String sortField, String sortOrder, boolean recoverOngoing, boolean recoverCompleted) {

    StringBuilder query = new StringBuilder();

    if (search != null && !search.trim().isEmpty()) {
      query.append(" AND to_char(image_date, 'YYYY-MM-DD') LIKE '" + search + "%'");
    } else if (recoverOngoing) {
      query.append(" AND (state <> '" + ImageTaskState.ARCHIVED.getValue() + "' AND state <> '"
          + ImageTaskState.FAILED.getValue() + "') ");
    } else if (recoverCompleted) {
      query.append(" AND (state = '" + ImageTaskState.ARCHIVED.getValue() + "' OR state = '"
          + ImageTaskState.FAILED.getValue() + "') ");
    } else if (state != null) {
      query.append(" AND state = '" + state.getValue() + "' ");
    }

    if (sortField != null && sortOrder != null && !sortField.trim().isEmpty() && !sortOrder.trim().isEmpty())
      query.append(" ORDER BY " + sortField + " " + sortOrder.toUpperCase());
    if (page > 0 && size > 0)
      query.append(" OFFSET " + (page - 1) * size + " ROWS FETCH NEXT " + size + " ROWS ONLY");

    return query.toString();
  }

  @Override
  public List<SapsImage> getUserJobTasks(
      String jobId,
      ImageTaskState state,
      String search,
      Integer page,
      Integer size,
      String sortField,
      String sortOrder,
      boolean recoverOngoing,
      boolean recoverCompleted) {
    if (jobId == null || jobId.isEmpty()) {
      LOGGER.error("invalid job id " + jobId);
      throw new IllegalArgumentException("Job id is missing");
    }

    StringBuilder query = new StringBuilder(JDBCCatalogConstants.Queries.Select.JOB_TASKS);
    query.append(addJobTasksFilter(search, state, page, size, sortField, sortOrder, recoverOngoing, recoverCompleted));

    PreparedStatement statement = null;
    Connection connection = null;

    try {
      connection = getConnection();

      statement = connection.prepareStatement(query.toString());
      statement.setString(1, jobId);
      statement.setQueryTimeout(300);
      statement.execute();

      ResultSet rs = statement.getResultSet();
      return JDBCCatalogUtil.extractSapsTasks(rs);
    } catch (SQLException e) {
      throw new CatalogException("Error while extract all tasks");
    } catch (JDBCCatalogException e) {
      throw new CatalogException("Error while extract all tasks");
    } finally {
      close(statement, connection);
    }
  }

  @Override
  public Integer getUserJobTasksCount(String jobId, ImageTaskState state, String search, boolean recoverOngoing, boolean recoverCompleted) {

    StringBuilder query = new StringBuilder(JDBCCatalogConstants.Queries.Select.JOB_TASKS_COUNT);
    query.append(addJobTasksFilter(search, state, 0, 0, null, null, recoverOngoing, recoverCompleted));

    PreparedStatement statement = null;
    Connection connection = null;

    try {
      connection = getConnection();
      statement = connection.prepareStatement(query.toString());
      statement.setString(1, jobId);
      statement.setQueryTimeout(300);
      statement.execute();

      ResultSet rs = statement.getResultSet();
      rs.next();
      return rs.getInt("count");
    } catch (SQLException e) {
      throw new CatalogException("Error while select all tasks");
    } finally {
      close(statement, connection);
    }
  }
}
