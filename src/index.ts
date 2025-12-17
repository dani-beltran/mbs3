#!/usr/bin/env node
import { program } from "commander";
import { runListBackups } from "./commands/list";
import { runRestore } from "./commands/restore";
import { runDump } from "./commands/dump";
import { runPrune } from "./commands/prune";

// Get package version dynamically
const packageJson = require("../package.json");

program
  .name("mbs3")
  .description("CLI tool to backup and restore MongoDB databases using S3 as storage")
  .version(packageJson.version, "-v, --version", "Output the current version");

program
  .command("dump")
  .description("Create a backup of the MongoDB database and upload to S3")
  .option("-k, --keep-local", "Keep local backup files after upload", false)
  .option("-B, --bucket <name>", "S3 bucket name (overrides S3_BUCKET env var)")
  .option("-P, --prefix <path>", "S3 prefix path (overrides S3_PREFIX env var)")
  .option("-D, --database <name>", "MongoDB database name (overrides MONGODB_DATABASE env var)")
  .action(runDump);

program
  .command("restore")
  .description("Restore a MongoDB database from an S3 backup")
  .option("-b, --backup <name>", "Specific backup folder name to restore (defaults to most recent)")
  .option("-d, --drop", "Drop existing collections before restore", false)
  .option("-k, --keep-local", "Keep downloaded backup files after restore", false)
  .option("-B, --bucket <name>", "S3 bucket name (overrides S3_BUCKET env var)")
  .option("-P, --prefix <path>", "S3 prefix path (overrides S3_PREFIX env var)")
  .option("-D, --database <name>", "MongoDB database name (overrides MONGODB_DATABASE env var)")
  .action(runRestore);

program
  .command("list")
  .description("List available backups in S3")
  .option("-B, --bucket <name>", "S3 bucket name (overrides S3_BUCKET env var)")
  .option("-P, --prefix <path>", "S3 prefix path (overrides S3_PREFIX env var)")
  .action(runListBackups);

program
  .command("prune")
  .description("Delete backups older than a specified number of days")
  .option("-d, --days <number>", "Number of days to keep (overrides PRUNE_DAYS env var, default: 14)", parseInt)
  .option("-B, --bucket <name>", "S3 bucket name (overrides S3_BUCKET env var)")
  .option("-P, --prefix <path>", "S3 prefix path (overrides S3_PREFIX env var)")
  .option("--dry-run", "Show what would be deleted without actually deleting", false)
  .action(runPrune);

program.parse();
