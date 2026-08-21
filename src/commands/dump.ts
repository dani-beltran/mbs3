import { loadConfig } from "../config";
import { cleanupLocalBackup, createMongoDump, DumpResult } from "../mongo/mongodump";
import { uploadToS3 } from "../s3/s3-upload";
import { create as tarCreate} from "tar";

export async function runDump(options: { keepLocal: boolean; bucket?: string; prefix?: string; database?: string }): Promise<void> {
  const startTime = Date.now();

  console.log("🚀 MongoDB Backup to S3 - Starting...\n");

  try {
    // Load configuration
    const config = loadConfig({ bucket: options.bucket, prefix: options.prefix, database: options.database });
    console.log(`📋 Configuration loaded`);
    console.log(`   Database: ${config.mongodb.database}`);
    console.log(`   S3 Bucket: ${config.s3.bucket}`);
    console.log(`   S3 Prefix: ${config.s3.prefix}\n`);

    // Create mongodump
    const dumpResult = await createMongoDump(config);

    // Compress the dump
    console.log("Compressing the dump...");
    const compressedDumpResult = await compressDump(dumpResult);
    console.log(`✅ Compressed dump created at: ${compressedDumpResult.outputPath}`);

    // Upload to S3
    const uploadResult = await uploadToS3(config, compressedDumpResult);

    // Cleanup local backup unless --keep-local is specified
    if (!options.keepLocal) {
      cleanupLocalBackup(dumpResult.outputPath);
    } else {
      console.log(`📁 Local backup kept at: ${dumpResult.outputPath}`);
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`\n✨ Backup completed successfully in ${duration}s`);
    console.log(`   Files: ${uploadResult.totalFiles}`);
    console.log(`   Location: s3://${uploadResult.bucket}/${uploadResult.key}`);

    process.exit(0);
  } catch (error) {
    console.error("\n❌ Backup failed:", error instanceof Error ? error.message : error);
    process.exit(1);
  }
}

function compressDump(dumpResult: DumpResult): Promise<DumpResult> {
  return new Promise((resolve, reject) => {
    const outputFilePath = `${dumpResult.outputPath}.tar.gz`;
    tarCreate(
      {
        gzip: true,
        file: outputFilePath,
        cwd: dumpResult.outputPath,
      },
      ["."]
    )
      .then(() => resolve({ ...dumpResult, outputPath: outputFilePath }))
      .catch((err: Error) => reject(err));
  });
}