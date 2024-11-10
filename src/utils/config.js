import dotenv from 'dotenv'
dotenv.config()

export const PORT = process.env.PORT
let mongoUri = process.env.DEV_MONGODB_URI


export const MONGODB_URI = mongoUri
