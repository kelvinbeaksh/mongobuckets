import { ExpiryModel } from "../models/expiry.model.js"

class Buckets {
  
  minuteBucket = []

  fiveMinuteBucket = {}

  restBucket = {}

  constructor(){
  }

  addToMinuteBucket(id){
    this.minuteBucket.push(id)
  }

  addToFiveMinuteBucket(id, expireAt){
    this.fiveMinuteBucket[id] = expireAt
  }

  addToRestBucket(id, expireAt){
    this.restBucket[id] = expireAt
  }

  async resetMinuteBucket(){
    await ExpiryModel.deleteMany({ _id: this.minuteBucket })
    this.minuteBucket = []

    let currentTime = Date.now()
    Object.entries(this.fiveMinuteBucket).forEach(item => {
      const [id, deletionTime] = item
      if(deletionTime - currentTime < 60000 ){
        this.minuteBucket.push(id)
        delete this.fiveMinuteBucket[id]
      }
    })

  }


  resetFiveMinuteBucket(){
    let currentTime = Date.now()

    Object.entries(this.restBucket).forEach(item => {
      const [id, deletionTime] = item
      if(deletionTime - currentTime < 60000*5 ){
        this.fiveMinuteBucket[id] = deletionTime;
        delete this.restBucket[id]
      }
    })

  }
}

const buckets = new Buckets()

export {buckets}