import dotenv from "dotenv";

dotenv.config();

export interface Config {
  mongodb: {
    uri: string;
    database: string;
  };
  aws: {
    accessKeyId: string;
    secretAccessKey: string;
    region: string;
    endpoint?: string;
  };
  s3: {
    bucket: string;
    prefix: string;
  };
  mongodumpPath: string;
}

function getEnvOrThrow(key: string): string {
  const value = process.env[key];
  if (!value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

export function loadConfig(): Config {
  let auth = '';

  if (process.env.MONGO_INITDB_ROOT_USERNAME) {
    auth = `${process.env.MONGO_INITDB_ROOT_USERNAME}:${process.env.MONGO_INITDB_ROOT_PASSWORD}@`;
  }

  return {
    mongodb: {
      uri: `mongodb://${auth}${process.env.MONGODB_HOST || "localhost:27017"}`,
      database: getEnvOrThrow("MONGODB_DATABASE"),
    },
    aws: {
      accessKeyId: getEnvOrThrow("AWS_ACCESS_KEY_ID"),
      secretAccessKey: getEnvOrThrow("AWS_SECRET_ACCESS_KEY"),
      region: process.env.AWS_REGION || "us-east-1",
      endpoint: process.env.AWS_ENDPOINT || undefined,
    },
    s3: {
      bucket: getEnvOrThrow("S3_BUCKET"),
      prefix: process.env.S3_PREFIX || "backups/mongodb",
    },
    mongodumpPath: process.env.MONGODUMP_PATH || "mongodump",
  };
}
