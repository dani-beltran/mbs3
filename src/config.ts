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
  return {
    mongodb: {
      uri: buildMongoUri(),
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

function buildMongoUri(): string {
  let auth = '';
  let host = process.env.MONGODB_HOST || 'localhost:27017';
  host = host.endsWith('/') ? host.slice(0, -1) : host;
  let srv = process.env.MONGO_SRV ? '+srv' : '';
  let queryParams = '';

  if (process.env.MONGO_INITDB_ROOT_USERNAME) {
    const username = encodeURIComponent(process.env.MONGO_INITDB_ROOT_USERNAME);
    const password = encodeURIComponent(process.env.MONGO_INITDB_ROOT_PASSWORD || '');
    auth = `${username}:${password}@`;
  }

  // Add authSource if specified
  if (process.env.MONGO_AUTH_SOURCE) {
    queryParams = `authSource=${process.env.MONGO_AUTH_SOURCE}`;
  }
  return `mongodb${srv}://${auth}${host}/?${queryParams}`;
}