import mongoose from 'mongoose'
import {buckets} from '../entity/buckets.js';

export const NUMBER_OF_OBJECTS = 1000000


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

const DeletionLogModel = mongoose.model(`DeletionLog${NUMBER_OF_OBJECTS}`, deletionLogSchema);
const ExpiryModel = mongoose.model(`Expiry`, expirySchema);

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
  }
  if (change.operationType === 'insert') {
    const currentTime = new Date();
    const documentId = change.documentKey._id;
    const postImage = change.fullDocument;
    const expireAt = postImage.expireAt
    const deletionTimeDiff = expireAt-currentTime
    if(deletionTimeDiff <= 60*1000){
      buckets.addToMinuteBucket(documentId.toString())
    }else if(deletionTimeDiff > 60*1000 && deletionTimeDiff <= 300000){
      buckets.addToFiveMinuteBucket(documentId, expireAt)
    }else if(deletionTimeDiff > 300000){
      buckets.addToRestBucket(documentId, expireAt)
    }


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
    const expireAt = new Date(currDate + Math.floor(Math.random() * 600000)+600000); // Random time within the next 10 minutes
    const expiryDoc = new ExpiryModel({
      value: `Document ${i + 1}`,
      expireAt: expireAt
    });
    documents.push(expiryDoc);
    if(documents.length >= 200000){
      await ExpiryModel.insertMany(documents);
      documents = []
    }
  }
  await ExpiryModel.insertMany(documents);
}

export { ExpiryModel, DeletionLogModel };
