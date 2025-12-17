import { S3Client, ListObjectsV2Command, DeleteObjectsCommand } from "@aws-sdk/client-s3";
import type { Config } from "../config";

export interface BackupWithDate {
  name: string;
  date: Date;
}

export interface DeleteResult {
  backupName: string;
  deletedFiles: number;
}

function createS3Client(config: Config): S3Client {
  return new S3Client({
    region: config.aws.region,
    credentials: {
      accessKeyId: config.aws.accessKeyId,
      secretAccessKey: config.aws.secretAccessKey,
    },
    ...(config.aws.endpoint && { endpoint: config.aws.endpoint }),
  });
}

/**
 * Extracts the date from a backup name.
 * Expected format: databaseName-YYYY-MM-DD-HH-mm-ss or similar timestamp patterns
 */
function extractDateFromBackupName(backupName: string): Date | null {
  // Try to match ISO-like timestamp pattern: YYYY-MM-DD-HH-mm-ss or YYYY-MM-DDTHH:mm:ss
  const timestampMatch = backupName.match(/(\d{4})-(\d{2})-(\d{2})[-T](\d{2})[-:](\d{2})[-:](\d{2})/);
  
  if (timestampMatch) {
    const [, year, month, day, hour, minute, second] = timestampMatch;
    return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}Z`);
  }

  // Try simpler date pattern: YYYY-MM-DD
  const dateMatch = backupName.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (dateMatch) {
    const [, year, month, day] = dateMatch;
    return new Date(`${year}-${month}-${day}T00:00:00Z`);
  }

  return null;
}

export async function listBackupsWithDates(config: Config): Promise<BackupWithDate[]> {
  const client = createS3Client(config);

  // Ensure prefix ends with / to list contents, not the folder itself
  const prefix = config.s3.prefix.endsWith("/") ? config.s3.prefix : `${config.s3.prefix}/`;

  console.log(`📋 Listing backups in s3://${config.s3.bucket}/${prefix}`);

  const command = new ListObjectsV2Command({
    Bucket: config.s3.bucket,
    Prefix: prefix,
    Delimiter: "/",
  });

  const response = await client.send(command);
  const prefixes = response.CommonPrefixes?.map((p) => p.Prefix || "") || [];

  // Extract backup folder names and dates
  const backups: BackupWithDate[] = [];
  
  for (const p of prefixes) {
    const parts = p.replace(prefix, "").split("/").filter(Boolean);
    const name = parts[0] || "";
    
    if (!name) continue;
    
    const date = extractDateFromBackupName(name);
    if (date) {
      backups.push({ name, date });
    } else {
      console.warn(`   ⚠️  Could not parse date from backup name: ${name}`);
    }
  }

  // Sort by date, most recent first
  backups.sort((a, b) => b.date.getTime() - a.date.getTime());

  return backups;
}

export async function deleteBackup(config: Config, backupName: string): Promise<DeleteResult> {
  const client = createS3Client(config);
  const s3Prefix = `${config.s3.prefix}/${backupName}`;

  // List all objects in the backup folder
  const listCommand = new ListObjectsV2Command({
    Bucket: config.s3.bucket,
    Prefix: s3Prefix,
  });

  const listResponse = await client.send(listCommand);
  const objects = listResponse.Contents || [];

  if (objects.length === 0) {
    throw new Error(`No backup found at s3://${config.s3.bucket}/${s3Prefix}`);
  }

  // Delete all objects in the backup folder
  const objectsToDelete = objects
    .filter((obj) => obj.Key)
    .map((obj) => ({ Key: obj.Key! }));

  if (objectsToDelete.length === 0) {
    return { backupName, deletedFiles: 0 };
  }

  // S3 DeleteObjects can delete up to 1000 objects at a time
  const batchSize = 1000;
  let totalDeleted = 0;

  for (let i = 0; i < objectsToDelete.length; i += batchSize) {
    const batch = objectsToDelete.slice(i, i + batchSize);
    
    const deleteCommand = new DeleteObjectsCommand({
      Bucket: config.s3.bucket,
      Delete: {
        Objects: batch,
        Quiet: true,
      },
    });

    await client.send(deleteCommand);
    totalDeleted += batch.length;
  }

  return {
    backupName,
    deletedFiles: totalDeleted,
  };
}
