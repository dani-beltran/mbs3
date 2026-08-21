import { spawn } from "child_process";
import path from "path";
import fs from "fs";
import type { Config } from "../config";

export interface DumpResult {
  outputPath: string;
  databaseName: string;
  timestamp: string;
}

function directoryContainsFiles(dirPath: string): boolean {
  for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
    if (entry.isFile()) {
      return true;
    }

    if (entry.isDirectory() && directoryContainsFiles(path.join(dirPath, entry.name))) {
      return true;
    }
  }

  return false;
}

export async function createMongoDump(config: Config): Promise<DumpResult> {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const backupDir = path.join(process.cwd(), "backups");
  const outputPath = path.join(backupDir, `${config.mongodb.database}-${timestamp}`);

  // Ensure backup directory exists
  if (!fs.existsSync(backupDir)) {
    fs.mkdirSync(backupDir, { recursive: true });
  }

  console.log(`📦 Starting mongodump for database: ${config.mongodb.database}`);
  console.log(`📁 Output path: ${outputPath}`);

  const args = [
    "--uri",
    config.mongodb.uri,
    "--db",
    config.mongodb.database,
    "--out",
    outputPath,
    "--gzip",
  ];

  return new Promise((resolve, reject) => {
    const mongodump = spawn(config.mongodumpPath, args);

    let stderr = "";

    mongodump.stdout.on("data", (data) => {
      console.log(`mongodump: ${data}`);
    });

    mongodump.stderr.on("data", (data) => {
      stderr += data.toString();
      // mongodump outputs progress to stderr, so we log it
      process.stderr.write(data);
    });

    mongodump.on("close", (code) => {
      if (code === 0) {
        if (!fs.existsSync(outputPath) || !directoryContainsFiles(outputPath)) {
          reject(
            new Error(
              `mongodump completed without producing backup files for database "${config.mongodb.database}". ` +
                "Verify MONGODB_DATABASE and make sure the database contains at least one collection."
            )
          );
          return;
        }

        console.log("✅ mongodump completed successfully");
        resolve({
          outputPath,
          databaseName: config.mongodb.database,
          timestamp,
        });
      } else {
        reject(new Error(`mongodump failed with code ${code}: ${stderr}`));
      }
    });

    mongodump.on("error", (err) => {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        reject(
          new Error(
            `mongodump not found. Please install MongoDB Database Tools or set MONGODUMP_PATH environment variable.`
          )
        );
      } else {
        reject(err);
      }
    });
  });
}

export function cleanupLocalBackup(outputPath: string): void {
  if (fs.existsSync(outputPath)) {
    fs.rmSync(outputPath, { recursive: true, force: true });
    console.log(`🧹 Cleaned up local backup: ${outputPath}`);
  }
}
