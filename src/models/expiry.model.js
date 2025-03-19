import mongoose from 'mongoose';
import { performance } from 'perf_hooks'; 
import { buckets } from '../entity/buckets.js';

export const NUMBER_OF_OBJECTS = 1000;

const expirySchema = new mongoose.Schema({
  value: { type: String, required: true },
  expireAt: {
    type: Date
  },
});

const deletionLogSchema = new mongoose.Schema({
  deletedDocumentId: { type: mongoose.Schema.Types.ObjectId, required: true },
  deletionTime: { type: Date, default: Date.now() },
  deletionTimeDiff: { type: Number }
});

const bucketComputationLogSchema = new mongoose.Schema({
  noOfObjects: { type: Number, required: true },
  computationTime: { type: Number }
});

const DeletionLogModel = mongoose.model(`BucketDeletionLog${NUMBER_OF_OBJECTS}`, deletionLogSchema);
const ExpiryModel = mongoose.model(`Expiry`, expirySchema);
const BucketComputationLogModel = mongoose.model(`BucketComputationLog`, bucketComputationLogSchema);

let totalComputationTime = 0;  // Variable to accumulate the total computation time

const changeStream = ExpiryModel.watch([], { 
  fullDocument: "updateLookup", 
  fullDocumentBeforeChange: "required" 
});

changeStream.on('change', async (change) => {
  if (change.operationType === 'delete') {
    const deletionTime = new Date();
    const documentId = change.documentKey._id;
    const preImage = change.fullDocumentBeforeChange;
    const deletionLog = new DeletionLogModel({
      deletedDocumentId: documentId,
      deletionTime: deletionTime,
      expireAt: change.expireAt, // Store the original expireAt time in the deletion log
      deletionTimeDiff: deletionTime - preImage.expireAt
    });

    await deletionLog.save();

    // After deletion, you can save the total computation time to the log (if needed)
    const computationLog = new BucketComputationLogModel({
      noOfObjects: NUMBER_OF_OBJECTS,
      computationTime: totalComputationTime
    });
    await computationLog.save();

    // Reset total computation time for the next batch
    totalComputationTime = 0; 
  }

  if (change.operationType === 'insert') {
    const startTime = performance.now();
    
    const currentTime = new Date();
    const documentId = change.documentKey._id;
    const postImage = change.fullDocument;
    const expireAt = postImage.expireAt;
    const deletionTimeDiff = expireAt - currentTime;

    // Measure the time taken for bucket management
    if (deletionTimeDiff <= 60 * 1000) {
      buckets.addToMinuteBucket(documentId.toString());
    } else if (deletionTimeDiff > 60 * 1000 && deletionTimeDiff <= 300000) {
      buckets.addToFiveMinuteBucket(documentId, expireAt);
    } else if (deletionTimeDiff > 300000) {
      buckets.addToRestBucket(documentId, expireAt);
    }

    const endTime = performance.now();
    const computationTime = (endTime - startTime) ?? 0;

    // Accumulate the total computation time
    totalComputationTime += computationTime;
  }
});

function millisToMinutesAndSeconds(millis) {
  var minutes = Math.floor(millis / 60000);
  var seconds = ((millis % 60000) / 1000).toFixed(0);
  return minutes + ":" + (seconds < 10 ? '0' : '') + seconds;
}


export async function generateExpiryDocuments(count) {
  let documents = [];
  const currDate = Date.now();

  for (let i = 0; i < count; i++) {
    let expireAt;

    // 50% of documents expire in 1 to 15 minutes
    // 50% of documents expire in 15 to 60 minutes
    // if (Math.random() < 0.5) {
    expireAt = new Date(currDate + Math.floor(Math.random() * 600000)+600000);
    // } else {
    //   expireAt = new Date(currDate + Math.floor(Math.random() * 2700000) + 900000); // 15 to 60 minutes
    // }

    const expiryDoc = new ExpiryModel({
      value: `Document ${i + 1}`,
      expireAt: expireAt
    });

    documents.push(expiryDoc);

    // Batch insert every 200,000 records
    if (documents.length >= 200000) {
      await ExpiryModel.insertMany(documents);
      documents = [];
    }
  }

  // Insert remaining documents
  await ExpiryModel.insertMany(documents);
}

export { ExpiryModel, DeletionLogModel, BucketComputationLogModel };
