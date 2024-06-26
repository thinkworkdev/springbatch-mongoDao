package org.springframework.batch.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

/**
 * Uses MongoTemplate to perform CRUD on Springbatch's Job Execution data to Mongo DB. <br/>
 * MongoTemplate needs to be set as a property during bean definition
 * 
 * @author Baruch S.
 * @author vfouzdar
 */
@Repository
public class MongoJobExecutionDao extends AbstractMongoDao implements JobExecutionDao {

    private static final Logger LOG = LoggerFactory.getLogger(MongoJobExecutionDao.class);

    private MongoTemplate       mongoTemplate;

    public void setMongoTemplate(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @PostConstruct
    public void init() {
        // db.JobExecution.createIndex( {jobInstanceId : 1});
        getCollection().createIndex(jobInstanceIdObj(1L));
        // db.JobExecution.createIndex( {jobExecutionId : 1});
        getCollection().createIndex(jobExecutionIdObj(1L));
        // db.JobExecution.createIndex( {createTime : -1});
        getCollection().createIndex(new BasicDBObject(CREATE_TIME_KEY, -1));
        getCollection().createIndex(new Document(JOB_EXECUTION_ID_KEY, 1).append(JOB_INSTANCE_ID_KEY, 1),
                new IndexOptions());
    }

    public void saveJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);
        jobExecution.incrementVersion();
        Long id = getNextId(JobExecution.class.getSimpleName(), mongoTemplate);
        save(jobExecution, id);
    }

    private void save(JobExecution jobExecution, Long id) {
        jobExecution.setId(id);
        Bson object = toDbObjectWithoutVersion(jobExecution);
        object = combine(object, set(VERSION_KEY, jobExecution.getVersion()));
        UpdateResult result = getCollection().updateOne(eq(JOB_EXECUTION_ID_KEY, id), object, new UpdateOptions().upsert(true));
        LOG.debug("Saved Job Execution: " + result.getMatchedCount() + " - " + result.getModifiedCount());
    }

    private Bson toDbObjectWithoutVersion(JobExecution jobExecution) {
        return combine(set(JOB_EXECUTION_ID_KEY, jobExecution.getId()),
                set(JOB_INSTANCE_ID_KEY, jobExecution.getJobId()), set(START_TIME_KEY, jobExecution.getStartTime()),
                set(END_TIME_KEY, jobExecution.getEndTime()), set(STATUS_KEY, jobExecution.getStatus().toString()),
                set(EXIT_CODE_KEY, jobExecution.getExitStatus().getExitCode()),
                set(EXIT_MESSAGE_KEY, jobExecution.getExitStatus().getExitDescription()),
                set(CREATE_TIME_KEY, jobExecution.getCreateTime()),
                set(LAST_UPDATED_KEY, jobExecution.getLastUpdated()));
    }

    private void validateJobExecution(JobExecution jobExecution) {

        Assert.notNull(jobExecution);
        Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
        Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
        Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
    }

    public synchronized void updateJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);

        Long jobExecutionId = jobExecution.getId();
        Assert.notNull(jobExecutionId,
                "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");

        Assert.notNull(jobExecution.getVersion(),
                "JobExecution version cannot be null. JobExecution must be saved before it can be updated");

        Integer version = jobExecution.getVersion() + 1;

        if (getCollection().find(jobExecutionIdObj(jobExecutionId)) == null) {
            throw new NoSuchObjectException("Invalid JobExecution, ID " + jobExecutionId + " not found.");
        }

//		DBObject object = toDbObjectWithoutVersion(jobExecution);
//		object.put(VERSION_KEY, version);
//		getCollection().update(
//				start().add(JOB_EXECUTION_ID_KEY, jobExecutionId)
//						.add(VERSION_KEY, jobExecution.getVersion()).get(),
//				object);

        Bson object = toDbObjectWithoutVersion(jobExecution);
        object = combine(object, set(VERSION_KEY, version));
        getCollection().updateOne(
                combine(eq(JOB_EXECUTION_ID_KEY, jobExecutionId), eq(VERSION_KEY, jobExecution.getVersion())), object, new UpdateOptions().upsert(true));

        // Avoid concurrent modifications...
//		DBObject lastError = mongoTemplate.getDb().getLastError();
//		if (!((Boolean) lastError.get(UPDATED_EXISTING_STATUS))) {
//			LOG.error("Update returned status {}", lastError);
//			DBObject existingJobExecution = getCollection().findOne(
//					jobExecutionIdObj(jobExecutionId),
//					new BasicDBObject(VERSION_KEY, 1));
//			if (existingJobExecution == null) {
//				throw new IllegalArgumentException(
//						"Can't update this jobExecution, it was never saved.");
//			}
//			Integer curentVersion = ((Integer) existingJobExecution
//					.get(VERSION_KEY));
//			throw new OptimisticLockingFailureException(
//					"Attempt to update job execution id=" + jobExecutionId
//							+ " with wrong version ("
//							+ jobExecution.getVersion()
//							+ "), where current version is " + curentVersion);
//		}

        jobExecution.incrementVersion();
    }

    public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
        Assert.notNull(jobInstance, "Job cannot be null.");
        Long id = jobInstance.getId();
        Assert.notNull(id, "Job Id cannot be null.");
        FindIterable<Document> documents = getCollection().find(jobInstanceIdObj(id))
                .sort(new BasicDBObject(JOB_EXECUTION_ID_KEY, -1));
        MongoCursor<Document> dbCursor = documents.iterator();
        List<JobExecution> result = new ArrayList<JobExecution>();
        while (dbCursor.hasNext()) {
            Document dbObject = dbCursor.next();
            result.add(mapJobExecution(jobInstance, dbObject));
        }
        return result;
    }

    public JobExecution getLastJobExecution(JobInstance jobInstance) {
        Long id = jobInstance.getId();
        FindIterable<Document> documents = getCollection().find(jobInstanceIdObj(id))
                .sort(new BasicDBObject(CREATE_TIME_KEY, -1)).limit(1);
        MongoCursor<Document> dbCursor = documents.iterator();
        if (!dbCursor.hasNext()) {
            return null;
        } else {
            Document singleResult = dbCursor.next();
            if (dbCursor.hasNext()) {
                throw new IllegalStateException("There must be at most one latest job execution");
            }
            return mapJobExecution(jobInstance, singleResult);
        }
    }

    public Set<JobExecution> findRunningJobExecutions(String jobName) {
        MongoCursor<Document> instancesCursor = mongoTemplate.getCollection(JobInstance.class.getSimpleName())
                .find(combine(eq(MongoJobInstanceDao.JOB_NAME_KEY, jobName), jobInstanceIdObj(1L))).iterator();
        List<Long> ids = new ArrayList<Long>();
        while (instancesCursor.hasNext()) {
            ids.add((Long) instancesCursor.next().get(JOB_INSTANCE_ID_KEY));
        }
        FindIterable<Document> documents = getCollection().find(
                combine(eq(JOB_INSTANCE_ID_KEY, new BasicDBObject("$in", ids.toArray())), eq(END_TIME_KEY, null)));
        MongoCursor<Document> dbCursor = documents.iterator();
        Set<JobExecution> result = new HashSet<JobExecution>();
        while (dbCursor.hasNext()) {
            result.add(mapJobExecution(dbCursor.next()));
        }
        return result;
    }

    public JobExecution getJobExecution(Long executionId) {
        return mapJobExecution(getCollection().find(jobExecutionIdObj(executionId)).first());
    }

    public void synchronizeStatus(JobExecution jobExecution) {
        Long id = jobExecution.getId();
        Document jobExecutionObject = getCollection().find(jobExecutionIdObj(id)).first();
        int currentVersion = jobExecutionObject != null ? ((Integer) jobExecutionObject.get(VERSION_KEY)) : 0;
        if (currentVersion != jobExecution.getVersion()) {
            if (jobExecutionObject == null) {
                save(jobExecution, id);
                jobExecutionObject = getCollection().find(jobExecutionIdObj(id)).first();
            }
            String status = (String) jobExecutionObject.get(STATUS_KEY);
            jobExecution.upgradeStatus(BatchStatus.valueOf(status));
            jobExecution.setVersion(currentVersion);
        }
    }

    protected MongoCollection<Document> getCollection() {
        return mongoTemplate.getCollection(JobExecution.class.getSimpleName());
    }

    private JobExecution mapJobExecution(Document dbObject) {
        return mapJobExecution(null, dbObject);
    }

    private JobExecution mapJobExecution(JobInstance jobInstance, Document dbObject) {
        if (dbObject == null) {
            return null;
        }
        Long id = (Long) dbObject.get(JOB_EXECUTION_ID_KEY);
        JobExecution jobExecution;

        if (jobInstance == null) {
            jobExecution = new JobExecution(id);
        } else {
            JobParameters jobParameters = getJobParameters(jobInstance.getId(), mongoTemplate);
            jobExecution = new JobExecution(jobInstance, id, jobParameters, null);
        }
        jobExecution.setStartTime((Date) dbObject.get(START_TIME_KEY));
        jobExecution.setEndTime((Date) dbObject.get(END_TIME_KEY));
        jobExecution.setStatus(BatchStatus.valueOf((String) dbObject.get(STATUS_KEY)));
        jobExecution.setExitStatus(
                new ExitStatus(((String) dbObject.get(EXIT_CODE_KEY)), (String) dbObject.get(EXIT_MESSAGE_KEY)));
        jobExecution.setCreateTime((Date) dbObject.get(CREATE_TIME_KEY));
        jobExecution.setLastUpdated((Date) dbObject.get(LAST_UPDATED_KEY));
        jobExecution.setVersion((Integer) dbObject.get(VERSION_KEY));

        return jobExecution;
    }

}
