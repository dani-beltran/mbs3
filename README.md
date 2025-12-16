# MongoDB Backup to S3

A CLI tool to backup MongoDB databases using `mongodump` and upload the backups to Amazon S3.

## Prerequisites

- Node.js 18+
- MongoDB Database Tools (`mongodump`) installed and available in PATH
- AWS credentials with S3 write access

### Installing MongoDB Database Tools

**macOS:**
```bash
brew tap mongodb/brew
brew install mongodb-database-tools
```

**Ubuntu/Debian:**
```bash
wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-x86_64-100.9.0.deb
sudo dpkg -i mongodb-database-tools-ubuntu2204-x86_64-100.9.0.deb
```

**Docker:**
The mongodump tool is included in the official MongoDB images.

## Installation

```bash
npm install
```

## Configuration

Copy the example environment file and configure your settings:

```bash
cp .env.example .env
```

Edit `.env` with your configuration:

```env
# MongoDB connection
MONGODB_DATABASE=your_database_name

# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET=your-backup-bucket
S3_PREFIX=backups/mongodb

# Optional: Custom mongodump path
# MONGODUMP_PATH=/usr/local/bin/mongodump
```

## Usage

### Run a backup

```bash
npm run dump
```

### Keep local backup files

```bash
npm run dump -- --keep-local
```

### Development mode

```bash
npm run dev
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
docker build -t mongodb-backup-s3 .
```

### Run MongoDB with Docker Compose

```bash
docker compose up -d mongodb
```

### Run a backup with Docker Compose

```bash
docker compose --profile backup run --rm backup
```

## Restoring from Backup

Download the backup from S3 and use `mongorestore`:

```bash
# Download from S3
aws s3 cp --recursive s3://your-bucket/backups/mongodb/your_database-2024-01-15T10-30-00-000Z/ ./restore/

# Restore to MongoDB
mongorestore --uri "mongodb://localhost:27017" --gzip ./restore/
```

## License

MIT
