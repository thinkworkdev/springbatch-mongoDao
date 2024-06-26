package org.springframework.batch.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;

/**
 * Uses MongoTemplate to perform CRUD on Springbatch's Job Instance Data to Mongo DB. <br/>
 * MongoTemplate needs to be set as a property during bean definition
 * 
 * @author Baruch S.
 * @authoer vfouzdar
 */
@Repository
public class MongoJobInstanceDao extends AbstractMongoDao implements JobInstanceDao {

    private MongoTemplate mongoTemplate;

    public void setMongoTemplate(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @PostConstruct
    public void init() {
        // db.JobInstance.createIndex( {jobName : -1});
        getCollection().createIndex(new BasicDBObject(JOB_NAME_KEY, -1L));
        getCollection().createIndex(new BasicDBObject(JOB_INSTANCE_ID_KEY, 1L));
    }

    public JobInstance createJobInstance(String jobName, final JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        Assert.state(getJobInstance(jobName, jobParameters) == null, "JobInstance must not already exist");

        Long jobId = getNextId(JobInstance.class.getSimpleName(), mongoTemplate);

        JobInstance jobInstance = new JobInstance(jobId, jobName);

        jobInstance.incrementVersion();

        Map<String, JobParameter> jobParams = jobParameters.getParameters();
        Map<String, Object> paramMap = new HashMap<String, Object>(jobParams.size());
        for (Map.Entry<String, JobParameter> entry : jobParams.entrySet()) {
            paramMap.put(entry.getKey().replaceAll(DOT_STRING, DOT_ESCAPE_STRING), entry.getValue().getValue());
        }
        Bson object = combine(set(JOB_INSTANCE_ID_KEY, jobId), set(JOB_NAME_KEY, jobName),
                set(JOB_KEY_KEY, createJobKey(jobParameters)), set(VERSION_KEY, jobInstance.getVersion()),
                set(JOB_PARAMETERS_KEY, new BasicDBObject(paramMap)));
        getCollection().updateOne(eq(JOB_INSTANCE_ID_KEY, jobId), object, new UpdateOptions().upsert(true));
        return jobInstance;
    }

    public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        String jobKey = createJobKey(jobParameters);

        return mapJobInstance(getCollection().find(combine(eq(JOB_NAME_KEY, jobName), eq(JOB_KEY_KEY, jobKey))).first(),
                jobParameters);
    }

    public JobInstance getJobInstance(Long instanceId) {
        return mapJobInstance(getCollection().find(jobInstanceIdObj(instanceId)).first());
    }

    public JobInstance getJobInstance(JobExecution jobExecution) {
        Document instanceId = mongoTemplate.getCollection(JobExecution.class.getSimpleName())
                .find(combine(jobExecutionIdObj(jobExecution.getId()), jobInstanceIdObj(1L))).first();
        if (instanceId != null) {
            removeSystemFields(instanceId);
            return mapJobInstance(getCollection().find(instanceId).first());
        } 
        return null;
    }

    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        return mapJobInstances(getCollection().find(new BasicDBObject(JOB_NAME_KEY, jobName))
                .sort(jobInstanceIdObj(-1L)).skip(start).limit(count));
    }

    public List<String> getJobNames() {
        DistinctIterable<String> distinctIter = getCollection().distinct(JOB_NAME_KEY, String.class);
        MongoCursor<String> cursor = distinctIter.iterator();
        List<String> results = new ArrayList<>();
        while (cursor.hasNext()) {
            results.add(cursor.next());
        }

        Collections.sort(results);

        return results;

    }

    protected String createJobKey(JobParameters jobParameters) {

        Map<String, JobParameter> props = jobParameters.getParameters();
        StringBuilder stringBuilder = new StringBuilder();
        List<String> keys = new ArrayList<String>(props.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            stringBuilder.append(key).append("=").append(props.get(key).toString()).append(";");
        }

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available.  Fatal (should be in the JDK).");
        }

        try {
            byte[] bytes = digest.digest(stringBuilder.toString().getBytes("UTF-8"));
            return String.format("%032x", new BigInteger(1, bytes));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 encoding not available.  Fatal (should be in the JDK).");
        }
    }

    protected MongoCollection<Document> getCollection() {
        return mongoTemplate.getCollection(JobInstance.class.getSimpleName());
    }

    private List<JobInstance> mapJobInstances(FindIterable<Document> documents) {
        List<JobInstance> results = new ArrayList<JobInstance>();
        MongoCursor<Document> dbCursor = documents.iterator();
        while (dbCursor.hasNext()) {
            results.add(mapJobInstance(dbCursor.next()));
        }
        return results;
    }

    private JobInstance mapJobInstance(Document dbObject) {
        return mapJobInstance(dbObject, null);
    }

    private JobInstance mapJobInstance(Document dbObject, JobParameters jobParameters) {
        JobInstance jobInstance = null;
        if (dbObject != null) {
            Long id = (Long) dbObject.get(JOB_INSTANCE_ID_KEY);
            if (jobParameters == null) {
                jobParameters = getJobParameters(id, mongoTemplate);
            }

            jobInstance = new JobInstance(id, (String) dbObject.get(JOB_NAME_KEY)); // should always be at version=0
                                                                                    // because they never get updated
            jobInstance.incrementVersion();
        }
        return jobInstance;
    }

    @Override
    public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        List<JobInstance> result = new ArrayList<JobInstance>();
        List<JobInstance> jobInstances = mapJobInstances(
                getCollection().find(eq(JOB_NAME_KEY, jobName)).limit(count).sort(jobInstanceIdObj(-1L)));
        for (JobInstance instanceEntry : jobInstances) {
            String key = instanceEntry.getJobName();
            String curJobName = key;
            if (key.lastIndexOf("|") > 0) {
                curJobName = key.substring(0, key.lastIndexOf("|"));
            }
            if (curJobName.equals(jobName)) {
                result.add(instanceEntry);
            }
        }
        return result;
    }

    @Override
    public int getJobInstanceCount(String jobName) throws NoSuchJobException {

        int count = 0;
        List<JobInstance> jobInstances = mapJobInstances(
                getCollection().find(new BasicDBObject(JOB_NAME_KEY, jobName)).sort(jobInstanceIdObj(-1L)));
        for (JobInstance instanceEntry : jobInstances) {
            String key = instanceEntry.getJobName();
            String curJobName = key;
            if (key.lastIndexOf("|") > 0) {
                curJobName = key.substring(0, key.lastIndexOf("|"));
            }

            if (curJobName.equals(jobName)) {
                count++;
            }
        }

        if (count == 0) {
            throw new NoSuchJobException("No job instances for job name " + jobName + " were found");
        } else {
            return count;
        }
    }

    public long getLongJobInstanceCount(String jobName) {
        long countLong = getCollection().countDocuments(new BasicDBObject(JOB_NAME_KEY, jobName));
        return countLong;
    }

}
