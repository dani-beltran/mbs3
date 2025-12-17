# mbs3 - MongoDB Backup to S3 storage

A CLI tool to backup and restore MongoDB databases using `mongodump`/`mongorestore` and Amazon S3.

## Prerequisites

- Node.js 18+
- MongoDB Database Tools (`mongodump`, `mongorestore`) installed and available in PATH
- AWS credentials with S3 read/write access

### Installing MongoDB Database Tools

**macOS:**
```bash
brew tap mongodb/brew
brew install mongodb-database-tools
```

**Ubuntu/Debian (amd64):**
```bash
wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-x86_64-100.14.0.deb
sudo dpkg -i mongodb-database-tools-ubuntu2204-x86_64-100.14.0.deb
```

**Docker:**
The mongodump tool is included in the official MongoDB images.

**Other platforms:**
Refer to the [official MongoDB documentation](https://www.mongodb.com/docs/database-tools/installation/installation/) for installation instructions.

## Installation

### Using npx (no installation required)

You can run commands directly without installing:

```bash
npx mbs3 dump
npx mbs3 restore
npx mbs3 list
```

### Global Installation

Install globally to use anywhere:

```bash
npm install -g mbs3
```

Then use directly:

```bash
mbs3 dump
mbs3 restore
mbs3 list
```

### Local Installation

```bash
npm install mbs3
```

## Configuration

Create a `.env` file in the directory where you run the command:

```env
# MongoDB connection (required)
MONGODB_URI=mongodb://username:password@localhost:27017
MONGODB_DATABASE=your_database_name

# AWS S3 Configuration (required)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET=your-backup-bucket

# Optional: AWS region (default: us-east-1)
AWS_REGION=us-east-1
S3_PREFIX=backups/mongodb

# Optional: Custom S3-compatible endpoint (e.g., MinIO, DigitalOcean Spaces)
# AWS_ENDPOINT=https://your-custom-endpoint.com

# Optional: Custom mongodump/mongorestore path (if not in PATH)
# MONGODUMP_PATH=/usr/local/bin/mongodump
```

## Commands

### Dump (Create Backup)

Create a backup of the MongoDB database and upload to S3:

```bash
npx mbs3 dump
```

**Options:**
- `-k, --keep-local` - Keep local backup files after upload
- `-B, --bucket <name>` - S3 bucket name (overrides S3_BUCKET env var)
- `-P, --prefix <path>` - S3 prefix path (overrides S3_PREFIX env var)
- `-D, --database <name>` - MongoDB database name (overrides MONGODB_DATABASE env var)

```bash
# Basic dump
npx mbs3 dump

# Keep local backup files
npx mbs3 dump --keep-local

# Dump to a specific bucket and prefix
npx mbs3 dump --bucket my-backup-bucket --prefix backups/production

# Dump a specific database
npx mbs3 dump --database my-other-database
```

### Restore

Restore a MongoDB database from an S3 backup:

```bash
npx mbs3 restore
```

**Options:**
- `-b, --backup <name>` - Specific backup folder name to restore (defaults to most recent)
- `-d, --drop` - Drop existing collections before restore
- `-k, --keep-local` - Keep downloaded backup files after restore
- `-B, --bucket <name>` - S3 bucket name (overrides S3_BUCKET env var)
- `-P, --prefix <path>` - S3 prefix path (overrides S3_PREFIX env var)
- `-D, --database <name>` - MongoDB database name (overrides MONGODB_DATABASE env var)

```bash
# Restore most recent backup
npx mbs3 restore

# Restore specific backup
npx mbs3 restore --backup mydb-2024-01-15T10-30-00-000Z

# Drop existing collections and restore
npx mbs3 restore --drop

# Keep downloaded files after restore
npx mbs3 restore --keep-local

# Restore from a specific bucket and prefix
npx mbs3 restore --bucket my-backup-bucket --prefix backups/production

# Restore to a specific database
npx mbs3 restore --database my-other-database --drop
```

### List Backups

List all available backups in S3:

```bash
npx mbs3 list
```

**Options:**
- `-B, --bucket <name>` - S3 bucket name (overrides S3_BUCKET env var)
- `-P, --prefix <path>` - S3 prefix path (overrides S3_PREFIX env var)

```bash
# List backups from a specific bucket
npx mbs3 list --bucket my-backup-bucket

# List backups from a specific prefix
npx mbs3 list --prefix backups/production
```

### Version

Check the installed version:

```bash
npx mbs3 --version
```

### Help

Get help for any command:

```bash
npx mbs3 --help
npx mbs3 dump --help
npx mbs3 restore --help
```

## Output

Backups are:
1. Created locally using `mongodump` with gzip compression
2. Uploaded to S3 at: `s3://{bucket}/{prefix}/{database}-{timestamp}/`
3. Local files are cleaned up after successful upload (unless `--keep-local` is specified)

## S3 Structure

```
s3://your-bucket/
└── backups/
    └── mongodb/
        └── your_database-2024-01-15T10-30-00-000Z/
            └── your_database/
                ├── collection1.bson.gz
                ├── collection1.metadata.json.gz
                ├── collection2.bson.gz
                └── collection2.metadata.json.gz
```

## Docker

### Build the image

```bash
npm run build
docker build -t mbs3 .
```

### Run MongoDB with Docker Compose

```bash
docker compose up -d mongodb
```

### Run a backup with Docker Compose

```bash
docker compose --profile backup run --rm backup
```

## Development

### Local Development

```bash
# Install dependencies
npm install

# Run in development mode
npm run dev dump
npm run dev restore
npm run dev list

# Build the project
npm run build
```

### Publishing to npm

```bash
# Build and publish
npm publish
```

## License

MIT
