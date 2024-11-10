import { CronJob } from "cron";
import { buckets } from "../entity/buckets.js";

console.log('Before job instantiation');

export const minuteJob = new CronJob('0 */1 * * * *', function () {
	const d = new Date();
	console.log('At One Minute:', d);
  buckets.resetMinuteBucket()
});

export const fiveMinuteJob = new CronJob('0 */4 * * * *', function () {
	const d = new Date();
	console.log('At Five Minute:', d);
  buckets.resetFiveMinuteBucket()
});

console.log('After job instantiation');
