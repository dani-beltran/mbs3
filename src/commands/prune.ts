import { loadConfig } from "../config";
import { listBackupsWithDates, deleteBackup } from "../s3/s3-delete";

const DEFAULT_PRUNE_DAYS = 14;

export interface PruneOptions {
  bucket?: string;
  prefix?: string;
  days?: number;
  dryRun?: boolean;
}

function getPruneDays(optionDays?: number): number {
  if (optionDays !== undefined) {
    return optionDays;
  }
  const envDays = process.env.PRUNE_DAYS;
  if (envDays) {
    const parsed = parseInt(envDays, 10);
    if (!isNaN(parsed) && parsed > 0) {
      return parsed;
    }
    console.warn(`⚠️  Invalid PRUNE_DAYS value "${envDays}", using default ${DEFAULT_PRUNE_DAYS} days`);
  }
  return DEFAULT_PRUNE_DAYS;
}

export async function runPrune(options: PruneOptions): Promise<void> {
  const pruneDays = getPruneDays(options.days);
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - pruneDays);

  console.log(`🧹 MongoDB Backups - Pruning backups older than ${pruneDays} days\n`);
  console.log(`   Cutoff date: ${cutoffDate.toISOString()}\n`);

  if (options.dryRun) {
    console.log("   🔍 DRY RUN - No files will be deleted\n");
  }

  try {
    const config = loadConfig({ bucket: options.bucket, prefix: options.prefix });
    const backups = await listBackupsWithDates(config);

    if (backups.length === 0) {
      console.log("No backups found.");
      process.exit(0);
    }

    const backupsToDelete = backups.filter((b) => b.date < cutoffDate);
    const backupsToKeep = backups.filter((b) => b.date >= cutoffDate);

    console.log(`Found ${backups.length} backup(s):`);
    console.log(`   ✅ Keeping: ${backupsToKeep.length}`);
    console.log(`   🗑️  To delete: ${backupsToDelete.length}\n`);

    if (backupsToDelete.length === 0) {
      console.log("No backups to prune.");
      process.exit(0);
    }

    console.log("Backups to be deleted:");
    backupsToDelete.forEach((b) => {
      console.log(`   - ${b.name} (${b.date.toISOString()})`);
    });
    console.log();

    if (options.dryRun) {
      console.log("✅ Dry run complete. No files were deleted.");
      process.exit(0);
    }

    let deletedCount = 0;
    let errorCount = 0;

    for (const backup of backupsToDelete) {
      try {
        const result = await deleteBackup(config, backup.name);
        console.log(`   🗑️  Deleted: ${backup.name} (${result.deletedFiles} files)`);
        deletedCount++;
      } catch (error) {
        console.error(`   ❌ Failed to delete ${backup.name}:`, error instanceof Error ? error.message : error);
        errorCount++;
      }
    }

    console.log();
    console.log(`✅ Prune complete!`);
    console.log(`   Deleted: ${deletedCount} backup(s)`);
    if (errorCount > 0) {
      console.log(`   Errors: ${errorCount}`);
    }

    process.exit(errorCount > 0 ? 1 : 0);
  } catch (error) {
    console.error("\n❌ Failed to prune backups:", error instanceof Error ? error.message : error);
    process.exit(1);
  }
}
